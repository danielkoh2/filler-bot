package bots

import (
	dloblib "driftgo/dlob/types"
	"driftgo/utils"
	"math"
	"math/rand"
)

func SelectMakers(makerNodeMap map[string][]dloblib.IDLOBNode) map[string][]dloblib.IDLOBNode {
	selectedMakers := make(map[string][]dloblib.IDLOBNode)
	for len(selectedMakers) < MAX_MAKERS_PER_FILL && len(makerNodeMap) > 0 {
		maker := SelectMaker(makerNodeMap)
		if maker == "" {
			break
		}
		makerNodes := makerNodeMap[maker]
		selectedMakers[maker] = makerNodes
		delete(makerNodeMap, maker)
	}
	return selectedMakers
}

func SelectMaker(makerNodeMap map[string][]dloblib.IDLOBNode) string {
	if len(makerNodeMap) == 0 {
		return ""
	}

	var totalLiquidity uint64 = 0
	for _, dlobNodes := range makerNodeMap {
		totalLiquidity += GetMakerLiquidity(dlobNodes)
	}

	var probabilities []float64
	for _, dlobNodes := range makerNodeMap {
		probabilities = append(probabilities, GetProbability(dlobNodes, totalLiquidity))
	}

	makerIndex := 0
	random := rand.Intn(10000)
	var sum float64 = 0
	for i, probability := range probabilities {
		sum += probability
		if float64(random)/10000 < sum {
			makerIndex = i
			break
		}
	}
	keys := utils.MapKeys(makerNodeMap)
	return keys[makerIndex]

}

func GetProbability(dlobNodes []dloblib.IDLOBNode, totalLiquidity uint64) float64 {
	makerLiquidity := GetMakerLiquidity(dlobNodes)
	return math.Ceil(float64(makerLiquidity)*1000/float64(totalLiquidity)) / 1000
}

func GetMakerLiquidity(dlobNodes []dloblib.IDLOBNode) uint64 {
	var liquidity uint64 = 0
	for _, dlobNode := range dlobNodes {
		liquidity += dlobNode.GetOrder().BaseAssetAmount - dlobNode.GetOrder().BaseAssetAmountFilled
	}
	return liquidity
}
