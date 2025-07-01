package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	controllerruntime "sigs.k8s.io/controller-runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
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
	config := ctrl.GetConfigOrDie()
	config.QPS = 5000
	config.Burst = 5000
	mgr := lo.Must(controllerruntime.NewManager(config, controllerruntime.Options{Metrics: metricsserver.Options{BindAddress: "0"}}))

	// Define flags
	outputFile := lo.FromPtr(flag.String("o", "", "Output CSV file"))
	overwrite := lo.FromPtr(flag.Bool("f", false, "Force overwrite if file exists"))
	flag.Parse()

	// Check if file exists
	_, err := os.Stat(outputFile)
	fileExists := !os.IsNotExist(err)

	if fileExists && !overwrite && outputFile != "" {
		log.Fatalf("file %s already exists. Use -f flag to force overwrite\n", outputFile)
	}

	file := &os.File{}
	var multiWriter io.Writer
	if outputFile != "" {
		file, err = os.Create(outputFile)
		if err != nil {
			log.Fatalf("failed creating output file %s, %s\n", outputFile, err)
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

	fmt.Fprintf(multiWriter, "Event,Node,Duration (s)\n")
	nodeLaunchTime := &sync.Map{}

	nodeWatcher := &nodeWatcher{startTime: time.Now(), kubeClient: mgr.GetClient(), nodeLaunchTime: nodeLaunchTime, nodeReadyTime: &sync.Map{}, nodeSchedulableTime: &sync.Map{}}
	nodeClaimWatcher := &nodeClaimWatcher{startTime: time.Now(), kubeClient: mgr.GetClient(), nodeLaunchTime: nodeLaunchTime, nodeRegisteredTime: &sync.Map{}}
	lo.Must0(nodeWatcher.SetupWithManager(mgr))
	lo.Must0(nodeClaimWatcher.SetupWithManager(mgr))
	mgr.GetCache().IndexField(ctx, &v1.NodeClaim{}, "status.providerID", func(o client.Object) []string {
		nodeClaim := o.(*v1.NodeClaim)
		if nodeClaim.Status.ProviderID == "" {
			return nil
		}
		return []string{nodeClaim.Status.ProviderID}
	})
	lo.Must0(mgr.Start(ctx))
}

type nodeWatcher struct {
	writer              io.Writer
	startTime           time.Time
	kubeClient          client.Client
	nodeLaunchTime      *sync.Map
	nodeReadyTime       *sync.Map
	nodeSchedulableTime *sync.Map
	once                sync.Once
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
	cond := GetCondition(node, corev1.NodeReady)
	if cond.Status != corev1.ConditionTrue {
		return reconcile.Result{}, nil
	}
	if cond.LastTransitionTime.Time.Before(c.startTime) {
		return reconcile.Result{}, nil
	}
	launchTime, ok := c.nodeLaunchTime.Load(node.Name)
	if !ok {
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}
	diff, loaded := c.nodeReadyTime.LoadOrStore(node.Name, cond.LastTransitionTime.Time.Sub(launchTime.(time.Time)))
	if !loaded {
		fmt.Fprintf(c.writer, "NodeReady,%s,%d\n", node.Name, int(diff.(time.Duration).Truncate(time.Second).Seconds()))
	}
	if len(node.Spec.Taints) > 0 {
		return reconcile.Result{}, nil
	}
	diff, loaded = c.nodeSchedulableTime.LoadOrStore(node.Name, time.Since(launchTime.(time.Time)))
	if !loaded {
		fmt.Fprintf(c.writer, "NodeSchedulable,%s,%d\n", node.Name, int(diff.(time.Duration).Truncate(time.Second).Seconds()))
	}
	return reconcile.Result{}, nil
}

func (c *nodeWatcher) SetupWithManager(mgr manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(mgr).
		Named(c.Name()).
		For(&corev1.Node{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 5000, CacheSyncTimeout: time.Minute * 5}).
		WithEventFilter(predicate.Funcs{
			UpdateFunc: func(o event.TypedUpdateEvent[client.Object]) bool {
				oldNode := o.ObjectOld.(*corev1.Node)
				newNode := o.ObjectNew.(*corev1.Node)
				return (len(newNode.Spec.Taints) == 0 && len(oldNode.Spec.Taints) != 0) ||
					(GetCondition(oldNode, corev1.NodeReady).Status != corev1.ConditionTrue && GetCondition(newNode, corev1.NodeReady).Status == corev1.ConditionTrue)
			},
		}).
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
	if !nodeClaim.StatusConditions().Get(v1.ConditionTypeLaunched).IsTrue() {
		return reconcile.Result{}, nil
	}
	c.nodeLaunchTime.LoadOrStore(nodeClaim.Status.NodeName, nodeClaim.StatusConditions().Get(v1.ConditionTypeLaunched).LastTransitionTime.Time)
	if nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered).IsTrue() && nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered).LastTransitionTime.Time.After(c.startTime) {
		registeredTime, loaded := c.nodeRegisteredTime.LoadOrStore(nodeClaim.Status.NodeName, nodeClaim.StatusConditions().Get(v1.ConditionTypeRegistered).LastTransitionTime.Time)
		if !loaded {
			fmt.Fprintf(c.writer, "NodeRegistered,%s,%d\n", nodeClaim.Status.NodeName, int(registeredTime.(time.Time).Sub(nodeClaim.StatusConditions().Get(v1.ConditionTypeLaunched).LastTransitionTime.Time).Truncate(time.Second).Seconds()))
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
