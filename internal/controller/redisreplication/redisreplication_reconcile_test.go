package redisreplication

import (
	"context"
	"testing"
	"time"

	commonapi "github.com/OT-CONTAINER-KIT/redis-operator/api/common/v1beta2"
	rrvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redisreplication/v1beta2"
	rsvb2 "github.com/OT-CONTAINER-KIT/redis-operator/api/redissentinel/v1beta2"
	"github.com/OT-CONTAINER-KIT/redis-operator/internal/k8sutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeStatefulSets struct{ ready bool }

func (f fakeStatefulSets) IsStatefulSetReady(context.Context, string, string) bool { return f.ready }

func (f fakeStatefulSets) GetStatefulSetReplicas(context.Context, string, string) int32 { return 3 }

func newSentinelReplication() *rrvb2.RedisReplication {
	return &rrvb2.RedisReplication{
		ObjectMeta: metav1.ObjectMeta{Name: "matrix-redis", Namespace: "matrix"},
		Spec: rrvb2.RedisReplicationSpec{
			Size:     ptr.To[int32](3),
			Sentinel: &rrvb2.Sentinel{Size: 3},
		},
	}
}

func TestReconcileRedisSkipsSetupWhileStatefulSetsNotReady(t *testing.T) {
	r := &Reconciler{StatefulSet: fakeStatefulSets{ready: false}}

	result, err := r.reconcileRedis(context.Background(), newSentinelReplication())

	require.NoError(t, err)
	assert.False(t, result.Requeue)
	assert.Zero(t, result.RequeueAfter)
}

type fakeHealer struct{ labelled []string }

func (f *fakeHealer) SentinelMonitor(context.Context, *rsvb2.RedisSentinel, string) error { return nil }

func (f *fakeHealer) SentinelSet(context.Context, *rsvb2.RedisSentinel, string) error { return nil }

func (f *fakeHealer) SentinelReset(context.Context, *rsvb2.RedisSentinel) error { return nil }

func (f *fakeHealer) UpdateRedisRoleLabel(context.Context, string, map[string]string, *commonapi.ExistingPasswordSecret, *commonapi.TLSConfig) error {
	return nil
}

func (f *fakeHealer) UpdateRedisReplicationRoleLabels(_ context.Context, _ string, _ map[string]string, masterPod string) error {
	f.labelled = append(f.labelled, masterPod)
	return nil
}

type statusCase struct {
	instance       *rrvb2.RedisReplication
	topology       k8sutils.RedisReplicationTopology
	realMaster     string
	sentinelMaster string
}

func runReconcileStatus(t *testing.T, c statusCase) (result reconcileOutcome) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, rrvb2.AddToScheme(scheme))
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(c.instance).WithStatusSubresource(c.instance).Build()
	healer := &fakeHealer{}
	r := &Reconciler{
		Client: cl,
		Healer: healer,
		RedisReplicationTopology: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication) (k8sutils.RedisReplicationTopology, error) {
			return c.topology, nil
		},
		RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string {
			result.calls = append(result.calls, "real-master")
			return c.realMaster
		},
		SentinelMonitoredMaster: func(context.Context, *rrvb2.RedisReplication, []string) (string, error) {
			result.calls = append(result.calls, "sentinel")
			return c.sentinelMaster, nil
		},
	}
	key := types.NamespacedName{Namespace: c.instance.Namespace, Name: c.instance.Name}
	stored := &rrvb2.RedisReplication{}
	require.NoError(t, cl.Get(context.Background(), key, stored))

	res, err := r.reconcileStatus(context.Background(), stored)
	require.NoError(t, err)

	after := &rrvb2.RedisReplication{}
	require.NoError(t, cl.Get(context.Background(), key, after))
	result.requeueAfter = res.RequeueAfter
	result.labelled = healer.labelled
	result.statusMaster = after.Status.MasterNode
	return result
}

type reconcileOutcome struct {
	calls        []string
	labelled     []string
	statusMaster string
	requeueAfter time.Duration
}

func TestReconcileStatusMovesTheLabelToThePromotedMaster(t *testing.T) {
	got := runReconcileStatus(t, statusCase{
		instance: newSentinelReplication(),
		topology: k8sutils.RedisReplicationTopology{
			Masters:    []string{"matrix-redis-0"},
			Slaves:     []string{"matrix-redis-2"},
			Unobserved: []string{"matrix-redis-1"},
		},
		realMaster: "matrix-redis-0",
	})

	assert.Equal(t, []string{"matrix-redis-0"}, got.labelled)
	assert.Equal(t, "matrix-redis-0", got.statusMaster)
	assert.Equal(t, []string{"real-master"}, got.calls)
	assert.Zero(t, got.requeueAfter)
}

func TestReconcileStatusAsksSentinelWhenThePromotedMasterHasNoReplica(t *testing.T) {
	got := runReconcileStatus(t, statusCase{
		instance: newSentinelReplication(),
		topology: k8sutils.RedisReplicationTopology{
			Masters:    []string{"matrix-redis-0"},
			Unobserved: []string{"matrix-redis-1", "matrix-redis-2"},
		},
		sentinelMaster: "matrix-redis-0",
	})

	assert.Equal(t, []string{"matrix-redis-0"}, got.labelled)
	assert.Equal(t, "matrix-redis-0", got.statusMaster)
	assert.Equal(t, []string{"real-master", "sentinel"}, got.calls)
}

func TestReconcileStatusLabelsTheOnlyPodOfASizeOneReplication(t *testing.T) {
	instance := newSentinelReplication()
	instance.Spec.Size = ptr.To[int32](1)
	instance.Spec.Sentinel = nil

	got := runReconcileStatus(t, statusCase{
		instance: instance,
		topology: k8sutils.RedisReplicationTopology{Masters: []string{"matrix-redis-0"}},
	})

	assert.Equal(t, []string{"matrix-redis-0"}, got.labelled)
	assert.Empty(t, got.calls)
}

func TestReconcileStatusChangesNothingWithoutAnIdentifiedMaster(t *testing.T) {
	instance := newSentinelReplication()
	instance.Status.MasterNode = "matrix-redis-1"

	got := runReconcileStatus(t, statusCase{
		instance: instance,
		topology: k8sutils.RedisReplicationTopology{
			Slaves:     []string{"matrix-redis-0", "matrix-redis-2"},
			Unobserved: []string{"matrix-redis-1"},
		},
	})

	assert.Empty(t, got.labelled)
	assert.Equal(t, "matrix-redis-1", got.statusMaster)
	assert.Equal(t, 5*time.Second, got.requeueAfter)
}

func TestReconcileStatusClearsTheMasterWhenTheCompleteViewHasNone(t *testing.T) {
	instance := newSentinelReplication()
	instance.Status.MasterNode = "matrix-redis-1"

	got := runReconcileStatus(t, statusCase{
		instance: instance,
		topology: k8sutils.RedisReplicationTopology{Slaves: []string{"matrix-redis-0", "matrix-redis-1", "matrix-redis-2"}},
	})

	assert.Empty(t, got.labelled)
	assert.Empty(t, got.statusMaster)
	assert.Equal(t, 5*time.Second, got.requeueAfter)
}

func TestReconcileRedisSkipsSetupOnAnIncompleteView(t *testing.T) {
	var calls []string
	r := &Reconciler{
		StatefulSet: fakeStatefulSets{ready: true},
		RedisReplicationTopology: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication) (k8sutils.RedisReplicationTopology, error) {
			return k8sutils.RedisReplicationTopology{
				Masters:    []string{"matrix-redis-0", "matrix-redis-1"},
				Unobserved: []string{"matrix-redis-2"},
			}, nil
		},
		RedisReplicationRealMaster: func(context.Context, kubernetes.Interface, *rrvb2.RedisReplication, []string) string {
			calls = append(calls, "real-master")
			return ""
		},
	}

	result, err := r.reconcileRedis(context.Background(), newSentinelReplication())

	require.NoError(t, err)
	assert.False(t, result.Requeue)
	assert.Empty(t, calls)
}
