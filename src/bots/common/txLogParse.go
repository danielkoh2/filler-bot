package common

import (
	"fmt"
	"regexp"
	"strconv"
)

func IsIxLog(line string) bool {
	matched, err := regexp.MatchString(`Program log: Instruction:`, line)
	return err == nil && matched
}

func IsEndIxLog(programId string, line string) bool {
	matched, err := regexp.MatchString(fmt.Sprintf(`Program %s consumed ([0-9]+) of ([0-9]+) compute units`, programId), line)
	return err == nil && matched
}

func IsFillIxLog(line string) bool {
	matched, err := regexp.MatchString(`Program log: Instruction: Fill(.*)Order`, line)
	return err == nil && matched
}

func IsArbIxLog(line string) bool {
	matched, err := regexp.MatchString(`Program log: Instruction: ArbPerp`, line)
	return err == nil && matched
}

func IsOrderDoesNotExistLog(line string) (bool, uint64) {
	pattern := regexp.MustCompile(`.*Order does not exist ([0-9]+)`)
	matches := pattern.FindStringSubmatch(line)
	if len(matches) == 0 {
		return false, 0
	}
	orderId, err := strconv.ParseUint(matches[1], 10, 64)
	if err != nil {
		return false, 0
	}
	return true, orderId
}

func IsMakerOrderDoesNotExistLog(line string) (bool, uint64) {
	pattern := regexp.MustCompile(`.*Maker has no order id ([0-9]+)`)
	matches := pattern.FindStringSubmatch(line)
	if len(matches) == 0 {
		return false, 0
	}
	orderId, err := strconv.ParseUint(matches[1], 10, 64)
	if err != nil {
		return false, 0
	}
	return true, orderId
}

// IsMakerBreachedMaintenanceMarginLog
/**
 * parses a maker breached maintenance margin log, returns the maker's userAccount pubkey if it exists
 * @param log
 * @returns the maker's userAccount pubkey if it exists, null otherwise
 */
func IsMakerBreachedMaintenanceMarginLog(line string) string {
	pattern := regexp.MustCompile(`.*maker \(([1-9A-HJ-NP-Za-km-z]+)\) breached (maintenance|fill) requirements.*$`)
	matches := pattern.FindStringSubmatch(line)
	if len(matches) == 0 {
		return ""
	}
	return matches[1]
}

func IsTakerBreachedMaintenanceMarginLog(line string) bool {
	matched, err := regexp.MatchString(`.*taker breached (maintenance|fill) requirements.*`, line)
	return err == nil && matched
}

func IsErrFillingLog(line string) (uint32, string, bool) {
	pattern := regexp.MustCompile(`.*Err filling order id ([0-9]+) for user ([a-zA-Z0-9]+)`)
	matches := pattern.FindStringSubmatch(line)
	if len(matches) == 0 {
		return 0, "", false
	}
	orderId, _ := strconv.ParseUint(matches[1], 10, 32)
	return uint32(orderId), matches[2], true
}

func IsErrArb(line string) bool {
	matched, err := regexp.MatchString(`.*NoArbOpportunity*`, line)
	return err == nil && matched
}

func IsErrArbNoBid(line string) bool {
	matched, err := regexp.MatchString(`.*NoBestBid*`, line)
	return err == nil && matched
}

func IsErrArbNoAsk(line string) bool {
	matched, err := regexp.MatchString(`.*NoBestAsk*`, line)
	return err == nil && matched
}

func IsErrStaleOracle(line string) bool {
	matched, err := regexp.MatchString(`.*Invalid Oracle: Stale.*`, line)
	return err == nil && matched
}
