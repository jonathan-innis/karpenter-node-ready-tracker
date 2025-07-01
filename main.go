package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/karpenter-provider-aws/pkg/batcher"
	"github.com/awslabs/operatorpkg/serrors"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/operator/logging"
)

var unhealthyCondition = corev1.NodeCondition{
	Type:   corev1.NodeConditionType("TestTypeReady"),
	Status: corev1.ConditionFalse,
}
var tolerationDuration = 30 * time.Second

func main() {
	ctrllog.SetLogger(logging.NopLogger)
	ctx := context.Background()
	cfg := ctrl.GetConfigOrDie()
	cfg.QPS = 5000
	cfg.Burst = 5000
	mgr := lo.Must(controllerruntime.NewManager(cfg, controllerruntime.Options{Metrics: metricsserver.Options{BindAddress: "0"}}))

	// Define flags
	outputFile := flag.String("o", "", "Output CSV file")
	overwrite := flag.Bool("f", false, "Force overwrite if file exists")
	flag.Parse()

	// Check if file exists
	_, err := os.Stat(lo.FromPtr(outputFile))
	fileExists := !os.IsNotExist(err)

	if fileExists && !lo.FromPtr(overwrite) && lo.FromPtr(outputFile) != "" {
		log.Fatalf("file %s already exists. Use -f flag to force overwrite\n", lo.FromPtr(outputFile))
	}

	file := &os.File{}
	var multiWriter io.Writer
	if lo.FromPtr(outputFile) != "" {
		file, err = os.Create(lo.FromPtr(outputFile))
		if err != nil {
			log.Fatalf("failed creating output file %s, %s\n", lo.FromPtr(outputFile), err)
			return
		}
		multiWriter = io.MultiWriter(
			file,      // Write to file
			os.Stdout, // Write to standard output
		)
	} else {
		multiWriter = io.MultiWriter(
			os.Stdout, // Write to standard output
		)
	}
	defer file.Close()

	fmt.Fprintf(multiWriter, "Event,Node,Timestamp\n")
	nodeLaunchTime := &sync.Map{}

	c := lo.Must(config.LoadDefaultConfig(ctx))
	ec2Client := ec2.NewFromConfig(c)

	nodeWatcher := &nodeWatcher{writer: multiWriter, describeInstancesBatcher: batcher.NewDescribeInstancesBatcher(ctx, ec2Client), startTime: time.Now(), kubeClient: mgr.GetClient(), nodeLaunchTime: nodeLaunchTime, awsNodeKubeProxyScheduledTime: &sync.Map{}, nodeReadyTime: &sync.Map{}, nodeSchedulableTime: &sync.Map{}}
	nodeClaimWatcher := &nodeClaimWatcher{writer: multiWriter, startTime: time.Now(), kubeClient: mgr.GetClient(), nodeLaunchTime: nodeLaunchTime, nodeRegisteredTime: &sync.Map{}}
	lo.Must0(nodeWatcher.SetupWithManager(mgr))
	lo.Must0(nodeClaimWatcher.SetupWithManager(mgr))
	mgr.GetCache().IndexField(ctx, &corev1.Pod{}, "spec.nodeName", func(o client.Object) []string {
		pod := o.(*corev1.Pod)
		if pod.Spec.NodeName == "" {
			return nil
		}
		return []string{pod.Spec.NodeName}
	})
	lo.Must0(mgr.Start(ctx))
}

type nodeWatcher struct {
	writer                        io.Writer
	describeInstancesBatcher      *batcher.DescribeInstancesBatcher
	startTime                     time.Time
	kubeClient                    client.Client
	nodeLaunchTime                *sync.Map
	awsNodeKubeProxyScheduledTime *sync.Map
	nodeReadyTime                 *sync.Map
	nodeSchedulableTime           *sync.Map
	once                          sync.Once
}

func (*nodeWatcher) Name() string {
	return "node.watcher"
}

func (c *nodeWatcher) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	c.once.Do(func() {
		log.Printf("Started Node Watcher...")
	})
	node := &corev1.Node{}
	if err := c.kubeClient.Get(ctx, request.NamespacedName, node); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	if _, ok := c.nodeSchedulableTime.Load(node.Name); ok {
		return reconcile.Result{}, nil
	}
	if node.Spec.ProviderID == "" {
		return reconcile.Result{}, nil
	}
	if node.CreationTimestamp.Time.Before(c.startTime) {
		return reconcile.Result{}, nil
	}
	instanceID, err := ParseInstanceID(node.Spec.ProviderID)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("parsing instance id, %w", err)
	}
	out, err := c.describeInstancesBatcher.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{instanceID},
	})
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("describing instances, %w", err)
	}
	if len(out.Reservations) != 1 && len(out.Reservations[0].Instances) != 1 {
		return reconcile.Result{}, fmt.Errorf("expected a single reservation from DescribeInstances")
	}
	instance := out.Reservations[0].Instances[0]
	launchTime, loaded := c.nodeLaunchTime.LoadOrStore(node.Name, lo.FromPtr(instance.LaunchTime))
	if !loaded {
		fmt.Fprintf(c.writer, "NodeLaunched,%s,%s\n", node.Name, launchTime.(time.Time).Format(time.RFC3339))
		fmt.Fprintf(c.writer, "NodeJoined,%s,%s\n", node.Name, node.CreationTimestamp.Time.Format(time.RFC3339))
	}
	awsNodePods := &corev1.PodList{}
	if err := c.kubeClient.List(ctx, awsNodePods, client.MatchingFields{"spec.nodeName": node.Name}, client.MatchingLabels{"app.kubernetes.io/name": "aws-node"}); err != nil {
		return reconcile.Result{}, err
	}
	if len(awsNodePods.Items) != 1 {
		return reconcile.Result{}, nil
	}
	kubeProxyPods := &corev1.PodList{}
	if err := c.kubeClient.List(ctx, kubeProxyPods, client.MatchingFields{"spec.nodeName": node.Name}, client.MatchingLabels{"k8s-app": "kube-proxy"}); err != nil {
		return reconcile.Result{}, err
	}
	if len(kubeProxyPods.Items) != 1 {
		return reconcile.Result{}, nil
	}
	awsNodeScheduled := GetPodCondition(&awsNodePods.Items[0], corev1.PodScheduled)
	kubeProxyScheduled := GetPodCondition(&kubeProxyPods.Items[0], corev1.PodScheduled)

	if awsNodeScheduled.Status != corev1.ConditionTrue {
		return reconcile.Result{}, nil
	}
	if kubeProxyScheduled.Status != corev1.ConditionTrue {
		return reconcile.Result{}, nil
	}
	newerTime := awsNodeScheduled.LastTransitionTime.Time
	if kubeProxyScheduled.LastTransitionTime.Time.After(newerTime) {
		newerTime = kubeProxyScheduled.LastTransitionTime.Time
	}
	awsNodeKubeProxyScheduled, loaded := c.awsNodeKubeProxyScheduledTime.LoadOrStore(node.Name, newerTime)
	if !loaded {
		fmt.Fprintf(c.writer, "AWSNodeKubeProxyScheduled,%s,%s\n", node.Name, awsNodeKubeProxyScheduled.(time.Time).Format(time.RFC3339))
	}
	cond := GetCondition(node, corev1.NodeReady)
	if cond.Status != corev1.ConditionTrue {
		return reconcile.Result{}, nil
	}
	nodeReady, loaded := c.nodeReadyTime.LoadOrStore(node.Name, cond.LastTransitionTime.Time)
	if !loaded {
		fmt.Fprintf(c.writer, "NodeReady,%s,%s\n", node.Name, nodeReady.(time.Time).Format(time.RFC3339))
	}
	if len(node.Spec.Taints) > 0 {
		return reconcile.Result{}, nil
	}
	nodeSchedulable, loaded := c.nodeSchedulableTime.LoadOrStore(node.Name, time.Now())
	if !loaded {
		fmt.Fprintf(c.writer, "NodeSchedulable,%s,%s\n", node.Name, nodeSchedulable.(time.Time).Format(time.RFC3339))
	}
	return reconcile.Result{}, nil
}

func (c *nodeWatcher) SetupWithManager(mgr manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(mgr).
		Named(c.Name()).
		For(&corev1.Node{}, builder.WithPredicates(predicate.Funcs{
			UpdateFunc: func(o event.TypedUpdateEvent[client.Object]) bool {
				oldNode := o.ObjectOld.(*corev1.Node)
				newNode := o.ObjectNew.(*corev1.Node)
				return (len(newNode.Spec.Taints) == 0 && len(oldNode.Spec.Taints) != 0) ||
					(GetCondition(oldNode, corev1.NodeReady).Status != corev1.ConditionTrue && GetCondition(newNode, corev1.NodeReady).Status == corev1.ConditionTrue)
			},
		})).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
			nodeName := o.(*corev1.Pod).Spec.NodeName
			if nodeName == "" {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: nodeName}}}
		})).
		WithOptions(controller.Options{MaxConcurrentReconciles: 5000, CacheSyncTimeout: time.Minute * 5}).
		Complete(c)
}

type nodeClaimWatcher struct {
	writer             io.Writer
	startTime          time.Time
	kubeClient         client.Client
	nodeLaunchTime     *sync.Map
	nodeRegisteredTime *sync.Map
	once               sync.Once
}

func (*nodeClaimWatcher) Name() string {
	return "nodeclaim.watcher"
}

func (c *nodeClaimWatcher) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	c.once.Do(func() {
		log.Printf("Started NodeClaim Watcher...")
	})
	nodeClaim := &v1.NodeClaim{}
	if err := c.kubeClient.Get(ctx, request.NamespacedName, nodeClaim); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}
	if nodeClaim.Status.NodeName == "" {
		return reconcile.Result{}, nil
	}
	if nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered).IsTrue() && nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered).LastTransitionTime.Time.After(c.startTime) {
		registeredTime, loaded := c.nodeRegisteredTime.LoadOrStore(nodeClaim.Status.NodeName, nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered).LastTransitionTime.Time)
		if !loaded {
			fmt.Fprintf(c.writer, "NodeRegistered,%s,%s\n", nodeClaim.Status.NodeName, registeredTime.(time.Time).Format(time.RFC3339))
		}
	}
	return reconcile.Result{}, nil
}

func (c *nodeClaimWatcher) SetupWithManager(mgr manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(mgr).
		Named(c.Name()).
		For(&v1.NodeClaim{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 5000, CacheSyncTimeout: time.Minute * 5}).
		WithEventFilter(predicate.Funcs{
			UpdateFunc: func(o event.TypedUpdateEvent[client.Object]) bool {
				return (o.ObjectNew.(*v1.NodeClaim).StatusConditions().Get(v1.ConditionTypeLaunched).IsTrue() && !o.ObjectOld.(*v1.NodeClaim).StatusConditions().Get(v1.ConditionTypeLaunched).IsTrue()) ||
					(o.ObjectNew.(*v1.NodeClaim).StatusConditions().Get(v1.ConditionTypeRegistered).IsTrue() && !o.ObjectOld.(*v1.NodeClaim).StatusConditions().Get(v1.ConditionTypeRegistered).IsTrue()) ||
					(o.ObjectOld.(*v1.NodeClaim).Status.NodeName == "" && o.ObjectNew.(*v1.NodeClaim).Status.NodeName != "")
			},
		}).
		Complete(c)
}

func GetCondition(n *corev1.Node, match corev1.NodeConditionType) corev1.NodeCondition {
	cond, _ := lo.Find(n.Status.Conditions, func(c corev1.NodeCondition) bool {
		return c.Type == match
	})
	return cond
}

func GetPodCondition(p *corev1.Pod, match corev1.PodConditionType) corev1.PodCondition {
	cond, _ := lo.Find(p.Status.Conditions, func(c corev1.PodCondition) bool {
		return c.Type == match
	})
	return cond
}

var (
	instanceIDRegex = regexp.MustCompile(`(?P<Provider>.*):///(?P<AZ>.*)/(?P<InstanceID>.*)`)
)

// ParseInstanceID parses the provider ID stored on the node to get the instance ID
// associated with a node
func ParseInstanceID(providerID string) (string, error) {
	matches := instanceIDRegex.FindStringSubmatch(providerID)
	if matches == nil {
		return "", serrors.Wrap(fmt.Errorf("provider id does not match known format"), "provider-id", providerID)
	}
	for i, name := range instanceIDRegex.SubexpNames() {
		if name == "InstanceID" {
			return matches[i], nil
		}
	}
	return "", serrors.Wrap(fmt.Errorf("provider id does not match known format"), "provider-id", providerID)
}
