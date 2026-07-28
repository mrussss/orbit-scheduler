package worker

import "testing"

func TestLifecycleStateValuesAreOrdered(t *testing.T) {
	if !(StateInitialized < StateRunning && StateRunning < StateDraining && StateDraining < StateStopping && StateStopping < StateStopped) {
		t.Fatal("lifecycle states must progress monotonically")
	}
}
