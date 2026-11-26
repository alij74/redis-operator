package redis

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/OT-CONTAINER-KIT/redis-operator/internal/controller/common"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (h *healer) UpdateRedisReplicationRoleLabels(ctx context.Context, ns string, labels map[string]string, masterPod string) error {
	if masterPod == "" {
		return nil
	}
	selector := make([]string, 0, len(labels))
	for key, value := range labels {
		selector = append(selector, fmt.Sprintf("%s=%s", key, value))
	}
	pods, err := h.k8s.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: strings.Join(selector, ","),
	})
	if err != nil {
		return err
	}
	if !slices.ContainsFunc(pods.Items, func(pod v1.Pod) bool { return pod.Name == masterPod }) {
		return nil
	}
	for _, pod := range pods.Items {
		if pod.Name != masterPod {
			if err := h.setRedisRoleLabel(ctx, ns, pod, common.RedisRoleLabelSlave); err != nil {
				return err
			}
		}
	}
	for _, pod := range pods.Items {
		if pod.Name == masterPod {
			return h.setRedisRoleLabel(ctx, ns, pod, common.RedisRoleLabelMaster)
		}
	}
	return nil
}

func (h *healer) setRedisRoleLabel(ctx context.Context, ns string, pod v1.Pod, role string) error {
	oldRole := pod.Labels[common.RedisRoleLabelKey]
	if oldRole == role {
		return nil
	}
	patch := []byte(fmt.Sprintf(`[{"op": "add", "path": "/metadata/labels/%s", "value": "%s"}]`, common.RedisRoleLabelKey, role))
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := h.k8s.CoreV1().Pods(ns).Patch(ctx, pod.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
		return err
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to update pod role label: %w", err)
	}
	log.FromContext(ctx).Info("updated pod role label", "pod", pod.Name, "oldRole", oldRole, "newRole", role)
	return nil
}
