package indexing

import (
	"math"
)

const ratio = 2

const channelSizeTaskTracker1 = 20
const channelSizeTaskTracker2 = 20
const channelSizeJobTracker = 100
const channelTaskTracker2Msg = 20

const MsgDismissWorker = "dismiss"
const MsgOrderingData = "ordering"
const MsgClearingData = "clearing"
const MsgSaveData2Disk = "saving"

const pathSaveJSON = "./static/pap-inverted-index.json"

const Dead = math.MaxInt32