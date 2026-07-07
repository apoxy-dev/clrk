/*
Copyright 2026 Apoxy, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/duration"
)

// This file is the single source of truth for the columns the clrk kinds print
// in table form. The in-tree apiserver builder (apoxy apiserver-runtime) type-
// asserts each result object to resourcestrategy.TableConverter, so implementing
// ConvertToTable here drives both `kubectl get <kind>` server-side AND the clrk
// CLI, which fetches typed objects via the generated clientset and renders the
// same metav1.Table locally. Keep the columns in sync with the CLI's renderer.

// tableAge renders an object's age the way kubectl does (a short human duration
// since creation). Matches the column the clrk CLI has always printed.
func tableAge(t metav1.Time) string {
	if t.IsZero() {
		return "<unknown>"
	}
	return duration.HumanDuration(time.Since(t.Time))
}

// dashIfEmpty renders an empty enum/string cell as "-" so a missing phase reads
// clearly rather than as a blank column.
func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// newTable returns a metav1.Table carrying the column headers unless the caller
// asked for headerless output (kubectl --no-headers passes TableOptions.NoHeaders).
func newTable(tableOptions runtime.Object, cols []metav1.TableColumnDefinition) *metav1.Table {
	t := &metav1.Table{}
	if opt, ok := tableOptions.(*metav1.TableOptions); !ok || !opt.NoHeaders {
		t.ColumnDefinitions = cols
	}
	return t
}

// TaskAgent.

var taskAgentColumns = []metav1.TableColumnDefinition{
	{Name: "Name", Type: "string", Format: "name", Description: "Name of the TaskAgent"},
	{Name: "Pool", Type: "string", Description: "WorkerPool that runs this agent's executions"},
	{Name: "Latest Ready", Type: "string", Description: "Most recent AgentSandboxRevision that became ready"},
	{Name: "Active", Type: "integer", Description: "In-flight (non-terminal) invocations"},
	{Name: "Warm", Type: "integer", Description: "Pre-warmed sandboxes held Ready across workers"},
	{Name: "Age", Type: "string", Description: "Time since creation"},
}

func taskAgentRow(ta *TaskAgent) metav1.TableRow {
	return metav1.TableRow{
		Cells: []interface{}{
			ta.Name,
			ta.Spec.WorkerPoolRef,
			ta.Status.LatestReadyRevisionName,
			int64(ta.Status.ActiveExecutions),
			int64(ta.Status.WarmSandboxes),
			tableAge(ta.CreationTimestamp),
		},
		Object: runtime.RawExtension{Object: ta},
	}
}

// ConvertToTable implements resourcestrategy.TableConverter.
func (ta *TaskAgent) ConvertToTable(ctx context.Context, tableOptions runtime.Object) (*metav1.Table, error) {
	t := newTable(tableOptions, taskAgentColumns)
	t.Rows = []metav1.TableRow{taskAgentRow(ta)}
	t.ResourceVersion = ta.ResourceVersion
	return t, nil
}

// ConvertToTable implements resourcestrategy.TableConverter for list responses.
func (l *TaskAgentList) ConvertToTable(ctx context.Context, tableOptions runtime.Object) (*metav1.Table, error) {
	t := newTable(tableOptions, taskAgentColumns)
	t.Rows = make([]metav1.TableRow, 0, len(l.Items))
	for i := range l.Items {
		t.Rows = append(t.Rows, taskAgentRow(&l.Items[i]))
	}
	t.ResourceVersion = l.ResourceVersion
	t.Continue = l.Continue
	t.RemainingItemCount = l.RemainingItemCount
	return t, nil
}

// DaemonAgent.

var daemonAgentColumns = []metav1.TableColumnDefinition{
	{Name: "Name", Type: "string", Format: "name", Description: "Name of the DaemonAgent"},
	{Name: "Pool", Type: "string", Description: "WorkerPool that runs this agent"},
	{Name: "Latest Ready", Type: "string", Description: "Most recent AgentSandboxRevision that became ready"},
	{Name: "Phase", Type: "string", Description: "Observed lifecycle phase"},
	{Name: "Restarts", Type: "integer", Description: "Number of daemon restarts"},
	{Name: "Age", Type: "string", Description: "Time since creation"},
}

func daemonAgentRow(da *DaemonAgent) metav1.TableRow {
	return metav1.TableRow{
		Cells: []interface{}{
			da.Name,
			da.Spec.WorkerPoolRef,
			da.Status.LatestReadyRevisionName,
			dashIfEmpty(string(da.Status.Phase)),
			int64(da.Status.RestartCount),
			tableAge(da.CreationTimestamp),
		},
		Object: runtime.RawExtension{Object: da},
	}
}

// ConvertToTable implements resourcestrategy.TableConverter.
func (da *DaemonAgent) ConvertToTable(ctx context.Context, tableOptions runtime.Object) (*metav1.Table, error) {
	t := newTable(tableOptions, daemonAgentColumns)
	t.Rows = []metav1.TableRow{daemonAgentRow(da)}
	t.ResourceVersion = da.ResourceVersion
	return t, nil
}

// ConvertToTable implements resourcestrategy.TableConverter for list responses.
func (l *DaemonAgentList) ConvertToTable(ctx context.Context, tableOptions runtime.Object) (*metav1.Table, error) {
	t := newTable(tableOptions, daemonAgentColumns)
	t.Rows = make([]metav1.TableRow, 0, len(l.Items))
	for i := range l.Items {
		t.Rows = append(t.Rows, daemonAgentRow(&l.Items[i]))
	}
	t.ResourceVersion = l.ResourceVersion
	t.Continue = l.Continue
	t.RemainingItemCount = l.RemainingItemCount
	return t, nil
}

// WorkerPool.

var workerPoolColumns = []metav1.TableColumnDefinition{
	{Name: "Name", Type: "string", Format: "name", Description: "Name of the WorkerPool"},
	{Name: "Replicas", Type: "integer", Description: "Desired number of worker pods"},
	{Name: "Ready", Type: "integer", Description: "Worker pods that are ready"},
	{Name: "Active", Type: "integer", Description: "In-flight (non-terminal) invocations across the pool"},
	{Name: "Age", Type: "string", Description: "Time since creation"},
}

func workerPoolRow(wp *WorkerPool) metav1.TableRow {
	var replicas int64
	if wp.Spec.Replicas != nil {
		replicas = int64(*wp.Spec.Replicas)
	}
	return metav1.TableRow{
		Cells: []interface{}{
			wp.Name,
			replicas,
			int64(wp.Status.ReadyReplicas),
			int64(wp.Status.ActiveExecutions),
			tableAge(wp.CreationTimestamp),
		},
		Object: runtime.RawExtension{Object: wp},
	}
}

// ConvertToTable implements resourcestrategy.TableConverter.
func (wp *WorkerPool) ConvertToTable(ctx context.Context, tableOptions runtime.Object) (*metav1.Table, error) {
	t := newTable(tableOptions, workerPoolColumns)
	t.Rows = []metav1.TableRow{workerPoolRow(wp)}
	t.ResourceVersion = wp.ResourceVersion
	return t, nil
}

// ConvertToTable implements resourcestrategy.TableConverter for list responses.
func (l *WorkerPoolList) ConvertToTable(ctx context.Context, tableOptions runtime.Object) (*metav1.Table, error) {
	t := newTable(tableOptions, workerPoolColumns)
	t.Rows = make([]metav1.TableRow, 0, len(l.Items))
	for i := range l.Items {
		t.Rows = append(t.Rows, workerPoolRow(&l.Items[i]))
	}
	t.ResourceVersion = l.ResourceVersion
	t.Continue = l.Continue
	t.RemainingItemCount = l.RemainingItemCount
	return t, nil
}

// CLRKConfig.

var clrkConfigColumns = []metav1.TableColumnDefinition{
	{Name: "Name", Type: "string", Format: "name", Description: "Name of the CLRKConfig"},
	{Name: "Email", Type: "string", Description: "Notifications signup email"},
	{Name: "Registered", Type: "string", Description: "Phone-home registration state"},
	{Name: "Age", Type: "string", Description: "Time since creation"},
}

func clrkConfigRow(c *CLRKConfig) metav1.TableRow {
	return metav1.TableRow{
		Cells: []interface{}{
			c.Name,
			dashIfEmpty(c.Spec.Notifications.Email),
			conditionState(c.Status.Notifications.Conditions, "Registered"),
			tableAge(c.CreationTimestamp),
		},
		Object: runtime.RawExtension{Object: c},
	}
}

// ConvertToTable implements resourcestrategy.TableConverter.
func (c *CLRKConfig) ConvertToTable(ctx context.Context, tableOptions runtime.Object) (*metav1.Table, error) {
	t := newTable(tableOptions, clrkConfigColumns)
	t.Rows = []metav1.TableRow{clrkConfigRow(c)}
	t.ResourceVersion = c.ResourceVersion
	return t, nil
}

// ConvertToTable implements resourcestrategy.TableConverter for list responses.
func (l *CLRKConfigList) ConvertToTable(ctx context.Context, tableOptions runtime.Object) (*metav1.Table, error) {
	t := newTable(tableOptions, clrkConfigColumns)
	t.Rows = make([]metav1.TableRow, 0, len(l.Items))
	for i := range l.Items {
		t.Rows = append(t.Rows, clrkConfigRow(&l.Items[i]))
	}
	t.ResourceVersion = l.ResourceVersion
	t.Continue = l.Continue
	t.RemainingItemCount = l.RemainingItemCount
	return t, nil
}

// conditionState returns the status of the named condition, or "-" when absent.
func conditionState(conds []metav1.Condition, condType string) string {
	for i := range conds {
		if conds[i].Type == condType {
			return string(conds[i].Status)
		}
	}
	return "-"
}
