package main

import (
	"context"
	"log"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/autoscaling"
	autoscaling_types "github.com/aws/aws-sdk-go-v2/service/autoscaling/types"
	"github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/buildbarn/bb-autoscaler/pkg/autoscaler"
	"github.com/buildbarn/bb-autoscaler/pkg/proto/configuration/bb_autoscaler"
	"github.com/buildbarn/bb-storage/pkg/cloud/aws"
	http_client "github.com/buildbarn/bb-storage/pkg/http/client"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	appsv1_apply "k8s.io/client-go/applyconfigurations/apps/v1"
	metav1_apply "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Autoscaler adjusts the capacity of Amazon EC2 Auto Scaling Groups (ASG),
// EKS Managed Node Groups, or Kubernetes Deployments based on Prometheus metrics.
type Autoscaler struct {
	configuration       *bb_autoscaler.ApplicationConfiguration
	prometheusAPI       v1.API
	autoScalingClient   *autoscaling.Client
	eksClient           *eks.Client
	kubernetesClientset *kubernetes.Clientset
}

// NewAutoscaler creates a new Autoscaler instance with all necessary clients
// initialized based on the provided configuration.
func NewAutoscaler(configuration *bb_autoscaler.ApplicationConfiguration) (*Autoscaler, error) {
	a := &Autoscaler{
		configuration: configuration,
	}

	// Prometheus client is always needed
	prometheusRoundTripper, err := http_client.NewRoundTripperFromConfiguration(configuration.PrometheusHttpClient)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create Prometheus HTTP client")
	}
	prometheusClient, err := api.NewClient(api.Config{
		Address:      configuration.PrometheusEndpoint,
		RoundTripper: prometheusRoundTripper,
	})
	if err != nil {
		return nil, util.StatusWrap(err, "Error creating Prometheus client")
	}
	a.prometheusAPI = v1.NewAPI(prometheusClient)

	// Initialize AWS and Kubernetes clients based on node group types
	for _, nodeGroup := range configuration.NodeGroups {
		switch nodeGroup.Kind.(type) {
		case *bb_autoscaler.NodeGroupConfiguration_AutoScalingGroupName, *bb_autoscaler.NodeGroupConfiguration_EksManagedNodeGroup:
			if a.autoScalingClient == nil {
				cfg, err := aws.NewConfigFromConfiguration(configuration.AwsSession, "Autoscaling")
				if err != nil {
					return nil, util.StatusWrap(err, "Failed to create AWS session")
				}
				a.autoScalingClient = autoscaling.NewFromConfig(cfg)
				a.eksClient = eks.NewFromConfig(cfg)
			}
		case *bb_autoscaler.NodeGroupConfiguration_KubernetesDeployment:
			if a.kubernetesClientset == nil {
				config, err := rest.InClusterConfig()
				if err != nil {
					return nil, util.StatusWrap(err, "Failed to create Kubernetes client configuration")
				}
				a.kubernetesClientset, err = kubernetes.NewForConfig(config)
				if err != nil {
					return nil, util.StatusWrap(err, "Failed to create Kubernetes client")
				}
			}
		default:
			return nil, status.Error(codes.InvalidArgument, "No ASG, EKS managed node group, or Kubernetes deployment name specified")
		}
	}

	return a, nil
}

// RunOnce executes a single autoscaling cycle: fetching metrics from Prometheus
// and adjusting node group sizes accordingly.
func (a *Autoscaler) RunOnce(ctx context.Context) error {
	// Obtain desired number of workers from Prometheus.
	log.Printf("[1/2] Fetching desired worker count from Prometheus by running query %#v", a.configuration.PrometheusQuery)
	result, _, err := a.prometheusAPI.Query(ctx, a.configuration.PrometheusQuery, time.Now())
	if err != nil {
		return util.StatusWrap(err, "Error querying Prometheus")
	}

	// Parse the metrics returned by Prometheus and convert them to
	// a map that's indexed by the platform.
	vector := result.(model.Vector)
	desiredWorkersMap := make(map[autoscaler.SizeClassQueueKey]float64, len(vector))
	for _, sample := range vector {
		sizeClassQueueKey, err := autoscaler.NewSizeClassQueueKeyFromMetric(sample.Metric)
		if err != nil {
			return util.StatusWrapf(err, "Metric %s", sample.Metric)
		}

		desiredWorkersMap[sizeClassQueueKey] = float64(sample.Value)
	}

	// Adjust node group and deployment sizes.
	log.Print("[2/2] Adjusting desired capacity of ASGs")

	for _, nodeGroup := range a.configuration.NodeGroups {
		platform, _ := protojson.Marshal(nodeGroup.Platform)
		log.Printf("Instance name prefix %#v platform %s size class %d", nodeGroup.InstanceNamePrefix, string(platform), nodeGroup.SizeClass)
		workersPerCapacityUnit := nodeGroup.WorkersPerCapacityUnit
		log.Print("Workers per capacity unit: ", workersPerCapacityUnit)

		// Obtain the desired number of workers from the
		// Prometheus metrics gathered previously.
		desiredWorkers, ok := desiredWorkersMap[autoscaler.NewSizeClassQueueKeyFromConfiguration(
			nodeGroup.InstanceNamePrefix,
			nodeGroup.Platform,
			nodeGroup.SizeClass,
		)]
		if ok {
			log.Print("Desired number of workers: ", desiredWorkers)

			// Obtain the minimum/maximum size of the ASG,
			// so that we can clamp the desired capacity.
			oldDesiredCapacity := int32(-1)
			var minSize, maxSize int32
			var primaryNodegroupOutput *eks.DescribeNodegroupOutput
			switch kind := nodeGroup.Kind.(type) {
			case *bb_autoscaler.NodeGroupConfiguration_AutoScalingGroupName:
				output, err := a.autoScalingClient.DescribeAutoScalingGroups(
					ctx,
					&autoscaling.DescribeAutoScalingGroupsInput{
						AutoScalingGroupNames: []string{kind.AutoScalingGroupName},
					})
				if err != nil {
					return util.StatusWrapf(err, "Failed to obtain properties of ASG %#v", kind.AutoScalingGroupName)
				}
				if len(output.AutoScalingGroups) != 1 {
					return status.Errorf(codes.FailedPrecondition, "Obtaining properties of ASG %#v returned %d entries", kind.AutoScalingGroupName, len(output.AutoScalingGroups))
				}
				asg := output.AutoScalingGroups[0]
				oldDesiredCapacity = *asg.DesiredCapacity
				minSize = *asg.MinSize
				maxSize = *asg.MaxSize
			case *bb_autoscaler.NodeGroupConfiguration_EksManagedNodeGroup:
				var err error
				primaryNodegroupOutput, err = a.eksClient.DescribeNodegroup(
					ctx,
					&eks.DescribeNodegroupInput{
						ClusterName:   &kind.EksManagedNodeGroup.ClusterName,
						NodegroupName: &kind.EksManagedNodeGroup.NodeGroupName,
					})
				if err != nil {
					return util.StatusWrapf(err, "Failed to obtain properties of EKS managed node group %#v in cluster %#v", kind.EksManagedNodeGroup.NodeGroupName, kind.EksManagedNodeGroup.ClusterName)
				}
				scalingConfig := primaryNodegroupOutput.Nodegroup.ScalingConfig
				oldDesiredCapacity = *scalingConfig.DesiredSize
				minSize = *scalingConfig.MinSize
				maxSize = *scalingConfig.MaxSize
			case *bb_autoscaler.NodeGroupConfiguration_KubernetesDeployment:
				minSize = kind.KubernetesDeployment.MinimumReplicas
				maxSize = kind.KubernetesDeployment.MaximumReplicas
			default:
				panic("Incomplete switch on node group kind")
			}
			log.Print("Node group minimum size: ", minSize)
			log.Print("Node group maximum size: ", maxSize)

			// Translate the desired number of workers to
			// the desired ASG capacity.
			newDesiredCapacity := (int32(math.Ceil(desiredWorkers)) + workersPerCapacityUnit - 1) / workersPerCapacityUnit
			if newDesiredCapacity < minSize {
				newDesiredCapacity = minSize
			}
			if newDesiredCapacity > maxSize {
				newDesiredCapacity = maxSize
			}

			// Apply the desired ASG capacity.
			if newDesiredCapacity == oldDesiredCapacity {
				log.Print("Leaving desired capacity at ", newDesiredCapacity)
			} else {
				if oldDesiredCapacity >= 0 {
					log.Printf("Changing desired capacity from %d to %d", oldDesiredCapacity, newDesiredCapacity)
				} else {
					log.Printf("Changing desired capacity to %d", newDesiredCapacity)
				}

				switch kind := nodeGroup.Kind.(type) {
				case *bb_autoscaler.NodeGroupConfiguration_AutoScalingGroupName:
					if _, err := a.autoScalingClient.SetDesiredCapacity(ctx, &autoscaling.SetDesiredCapacityInput{
						AutoScalingGroupName: &kind.AutoScalingGroupName,
						DesiredCapacity:      &newDesiredCapacity,
					}); err != nil {
						return util.StatusWrapf(err, "Failed to set desired capacity of ASG %#v", kind.AutoScalingGroupName)
					}
				case *bb_autoscaler.NodeGroupConfiguration_EksManagedNodeGroup:
					if _, err := a.eksClient.UpdateNodegroupConfig(ctx, &eks.UpdateNodegroupConfigInput{
						ClusterName:   &kind.EksManagedNodeGroup.ClusterName,
						NodegroupName: &kind.EksManagedNodeGroup.NodeGroupName,
						ScalingConfig: &types.NodegroupScalingConfig{
							DesiredSize: &newDesiredCapacity,
						},
					}); err != nil {
						return util.StatusWrapf(err, "Failed to set desired size of EKS managed node group %#v in cluster %#v", kind.EksManagedNodeGroup.NodeGroupName, kind.EksManagedNodeGroup.ClusterName)
					}
				case *bb_autoscaler.NodeGroupConfiguration_KubernetesDeployment:
					namespace := kind.KubernetesDeployment.Namespace
					name := kind.KubernetesDeployment.Name
					metaKind := "Deployment"
					metaAPIVersion := "apps/v1"
					if _, err := a.kubernetesClientset.
						AppsV1().
						Deployments(namespace).
						Apply(
							ctx,
							&appsv1_apply.DeploymentApplyConfiguration{
								TypeMetaApplyConfiguration: metav1_apply.TypeMetaApplyConfiguration{
									Kind:       &metaKind,
									APIVersion: &metaAPIVersion,
								},
								ObjectMetaApplyConfiguration: &metav1_apply.ObjectMetaApplyConfiguration{
									Name:      &name,
									Namespace: &namespace,
									Annotations: map[string]string{
										"kubernetes.io/change-cause": "replicas updated by bb_autoscaler",
									},
								},
								Spec: &appsv1_apply.DeploymentSpecApplyConfiguration{
									Replicas: &newDesiredCapacity,
								},
							},
							metav1.ApplyOptions{
								FieldManager: "bb_autoscaler",
								Force:        true,
							}); err != nil {
						return util.StatusWrapf(err, "Failed to change number of replicas of Kubernetes deployment %#v in namespace %#v", name, namespace)
					}
				default:
					panic("Incomplete switch on node group kind")
				}
			}

			// Handle secondary node group if configured (currently only for EKS managed node groups)
			if kind, ok := nodeGroup.Kind.(*bb_autoscaler.NodeGroupConfiguration_EksManagedNodeGroup); ok {
				if kind.EksManagedNodeGroup.SecondaryNodeGroupName != "" {
					if err := a.handleSecondaryNodeGroup(ctx, kind, primaryNodegroupOutput, newDesiredCapacity); err != nil {
						return err
					}
				}
			}
		} else {
			log.Print("WARNING: Prometheus did not return a desired number of workers")
		}
	}
	return nil
}

// handleSecondaryNodeGroup manages the secondary node group for failover scenarios.
//   - When the primary EKS node group has health issues (degraded status or health issues),
//     the secondary node group automatically scales up to fill the capacity gap.
//   - When primary is healthy, secondary scales down to its minimum size.
func (a *Autoscaler) handleSecondaryNodeGroup(
	ctx context.Context,
	kind *bb_autoscaler.NodeGroupConfiguration_EksManagedNodeGroup,
	primaryNodegroupOutput *eks.DescribeNodegroupOutput,
	newDesiredCapacity int32,
) error {
	log.Printf("Secondary node group configured: %#v", kind.EksManagedNodeGroup.SecondaryNodeGroupName)

	// Check if primary node group has health issues
	primaryHasIssues := false
	if primaryNodegroupOutput.Nodegroup.Status == types.NodegroupStatusDegraded {
		log.Printf("Primary node group %#v status is DEGRADED", kind.EksManagedNodeGroup.NodeGroupName)
		primaryHasIssues = true
	}
	if primaryNodegroupOutput.Nodegroup.Health != nil && len(primaryNodegroupOutput.Nodegroup.Health.Issues) > 0 {
		log.Printf("Primary node group %#v has %d health issue(s):", kind.EksManagedNodeGroup.NodeGroupName, len(primaryNodegroupOutput.Nodegroup.Health.Issues))
		for _, issue := range primaryNodegroupOutput.Nodegroup.Health.Issues {
			message := ""
			if issue.Message != nil {
				message = *issue.Message
			}
			log.Printf("  - Code: %v, Message: %s", issue.Code, message)
		}
		primaryHasIssues = true
	}

	// Get secondary node group configuration
	secondaryOutput, err := a.eksClient.DescribeNodegroup(
		ctx,
		&eks.DescribeNodegroupInput{
			ClusterName:   &kind.EksManagedNodeGroup.ClusterName,
			NodegroupName: &kind.EksManagedNodeGroup.SecondaryNodeGroupName,
		})
	if err != nil {
		return util.StatusWrapf(err, "Failed to obtain properties of secondary EKS managed node group %#v in cluster %#v", kind.EksManagedNodeGroup.SecondaryNodeGroupName, kind.EksManagedNodeGroup.ClusterName)
	}
	secondaryScalingConfig := secondaryOutput.Nodegroup.ScalingConfig
	secondaryMinSize := *secondaryScalingConfig.MinSize
	secondaryMaxSize := *secondaryScalingConfig.MaxSize
	secondaryOldDesired := *secondaryScalingConfig.DesiredSize

	// Calculate secondary desired capacity based on primary health
	var secondaryNewDesired int32
	if primaryHasIssues {
		// Primary has issues: query ASG to get actual in-service count
		if primaryNodegroupOutput.Nodegroup.Resources == nil || len(primaryNodegroupOutput.Nodegroup.Resources.AutoScalingGroups) == 0 {
			return status.Errorf(codes.FailedPrecondition, "No Auto Scaling Group found for primary node group %#v", kind.EksManagedNodeGroup.NodeGroupName)
		}
		primaryASGName := *primaryNodegroupOutput.Nodegroup.Resources.AutoScalingGroups[0].Name
		log.Printf("Primary node group backed by ASG: %#v", primaryASGName)

		primaryASGOutput, err := a.autoScalingClient.DescribeAutoScalingGroups(
			ctx,
			&autoscaling.DescribeAutoScalingGroupsInput{
				AutoScalingGroupNames: []string{primaryASGName},
			})
		if err != nil {
			return util.StatusWrapf(err, "Failed to describe ASG %#v for primary node group", primaryASGName)
		}
		if len(primaryASGOutput.AutoScalingGroups) != 1 {
			return status.Errorf(codes.FailedPrecondition, "Expected 1 ASG but got %d for ASG name %#v", len(primaryASGOutput.AutoScalingGroups), primaryASGName)
		}

		primaryASG := primaryASGOutput.AutoScalingGroups[0]
		primaryInService := int32(0)
		for _, instance := range primaryASG.Instances {
			if instance.LifecycleState == autoscaling_types.LifecycleStateInService {
				primaryInService++
			}
		}
		log.Printf("Primary ASG: Desired=%d, InService=%d", *primaryASG.DesiredCapacity, primaryInService)

		// Scale up secondary to fill the gap
		secondaryNewDesired = newDesiredCapacity - primaryInService
		if secondaryNewDesired < 0 {
			secondaryNewDesired = 0
		}
		log.Printf("Primary has issues. Primary in-service: %d, Total desired: %d, Secondary target: %d", primaryInService, newDesiredCapacity, secondaryNewDesired)
	} else {
		// Primary is healthy: scale down secondary to minimum
		secondaryNewDesired = secondaryMinSize
		log.Printf("Primary is healthy. Scaling secondary to minimum: %d", secondaryNewDesired)
	}

	// Clamp secondary to its min/max bounds
	if secondaryNewDesired < secondaryMinSize {
		secondaryNewDesired = secondaryMinSize
	}
	if secondaryNewDesired > secondaryMaxSize {
		secondaryNewDesired = secondaryMaxSize
	}

	// Update secondary node group if needed
	if secondaryNewDesired != secondaryOldDesired {
		log.Printf("Changing secondary node group %#v desired capacity from %d to %d", kind.EksManagedNodeGroup.SecondaryNodeGroupName, secondaryOldDesired, secondaryNewDesired)
		if _, err := a.eksClient.UpdateNodegroupConfig(ctx, &eks.UpdateNodegroupConfigInput{
			ClusterName:   &kind.EksManagedNodeGroup.ClusterName,
			NodegroupName: &kind.EksManagedNodeGroup.SecondaryNodeGroupName,
			ScalingConfig: &types.NodegroupScalingConfig{
				DesiredSize: &secondaryNewDesired,
			},
		}); err != nil {
			return util.StatusWrapf(err, "Failed to set desired size of secondary EKS managed node group %#v in cluster %#v", kind.EksManagedNodeGroup.SecondaryNodeGroupName, kind.EksManagedNodeGroup.ClusterName)
		}
	} else {
		log.Printf("Leaving secondary node group %#v desired capacity at %d", kind.EksManagedNodeGroup.SecondaryNodeGroupName, secondaryNewDesired)
	}

	return nil
}
