package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"pgregory.net/rapid"
)

// Idea is to use Property Testing strategy to generate the synthetic paylods for load testing
// Using the Payload spec specified in the problem statement, we create a rapid.Generator to
// produce values and finally build a JSON. I'm planning to build a few sets of 100K, 1M & 10M
// payloads that we can use with Vegeta (https://github.com/tsenart/vegeta) to generating the load
func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <number of probes>"+"\nWe would generate enough payloads for each probe to be able to run for upto 2 hours at 10 seconds delay", os.Args[0])
		os.Exit(1)
	}
	totalProbes, err := strconv.ParseInt(os.Args[1], 10, 32)
	if err != nil {
		log.Fatalln(err)
	}
	// Instead of getting this as an input, we know each probe sends data every 10 seconds
	// The problem statement defines the time duration of the test to be 120 minutes (aka)
	// 7200 seconds. So if we've our totalProbes, we will atleast need to generate
	// totalProbes * (7200 / 10) payloads assuming equal distribution among the probes, for us to
	// evaluate the system as described in the spec.
	totalPayloadsToGenerate := totalProbes * 720

	probeIds := make([]string, totalProbes)
	probeIdGen := rapid.StringMatching(`[a-zA-Z0-9]{3,100}`)
	for i := 0; i < int(totalProbes); i++ {
		probeIds[i] = probeIdGen.Example(i).(string)
	}

	measureNamesAndDescription := []string{
		"Spherical coordinate system - euclidean distance",
		"Spherical coordinate system - azimuth angle",
		"Spherical coordinate system - polar angle",
		"Localized electromagnetic frequency reading",
		"Probe lifespan estimate",
		"Probe diagnostic logs",
	}
	measureUnits := []string{"parsecs", "degrees", "degrees", "hz", "Years", "Text"}
	measureCodes := []string{"SCSED", "SCSEAA", "SCSEPA", "LER", "PLSE", "PDL"}
	measureValueDescriptions := []string{
		"Euclidean distance from earth", "Azimuth angle from earth",
		"Polar/Inclination angle from earth",
		"Electromagnetic frequency reading",
		"Number of years left in probe lifespan",
		"the diagnostic information from the probe",
	}
	measureTypes := []string{"Positioning", "Positioning", "Positioning", "Composition", "Probe", "Probe"}
	measureValueFloatGen := rapid.Float64Min(0.0)
	basicRunes := make([]rune, 0)
	basicRunes = append(basicRunes, rune(32), rune(95), rune(126)) // " ", _, ~
	for i := 48; i < 48+10; i++ {
		basicRunes = append(basicRunes, rune(i))
	}
	for i := 65; i < 65+26; i++ {
		basicRunes = append(basicRunes, rune(i))
	}
	for i := 65; i < 97+26; i++ {
		basicRunes = append(basicRunes, rune(i))
	}
	// I guess this is the value that gets changed for increasing the payload from 1Kb -> 20Kb?
	measureValueStringGen := rapid.StringOfN(rapid.RuneFrom(basicRunes), 32, 20480, -1)

	f, err := os.Create(fmt.Sprintf("%d-%d.payload", totalProbes, totalPayloadsToGenerate))
	if err != nil {
		log.Fatalln(err)
	}
	w := bufio.NewWriter(f)

	for i := 0; i < int(totalPayloadsToGenerate); i++ {
		payload := make(map[string]interface{})
		payload["probeId"] = probeIds[i%len(probeIds)] // round robin among all the probeIds that we have
		payload["eventId"] = uuid.NewString()
		payload["messageType"] = "spaceCartography"
		payload["eventTransmissionTime"] = time.Now().UnixMilli()
		measurements := make([]interface{}, 6)
		for m := 0; m < 6; m++ {
			measurement := make(map[string]interface{})
			measurement["measureName"] = measureNamesAndDescription[m]
			measurement["measureCode"] = measureCodes[m]
			measurement["measureUnit"] = measureUnits[m]
			if measureCodes[m] == "PDL" {
				measurement["measureValue"] = measureValueStringGen.Example()
				measurement["componentReading"] = 0.0
			} else {
				measurement["measureValue"] = measureValueFloatGen.Example()
				measurement["componentReading"] = measureValueFloatGen.Example()
			}
			measurement["measureValueDescription"] = measureValueDescriptions[m]
			measurement["measureType"] = measureTypes[m]

			measurements[m] = measurement
		}
		payload["messageData"] = measurements

		jsonInBytes, err := json.Marshal(payload)
		jsonInBytes = append(jsonInBytes, "\n"...)
		if err == nil {
			_, err = w.Write(jsonInBytes)
			if err != nil {
				log.Fatalln(err)
			}
		}
		if i%1000 == 0 {
			err = w.Flush()
			if err != nil {
				log.Fatalln(err)
			}
		}
	}

	err = w.Flush()
	if err != nil {
		log.Fatalln(err)
	}

	err = f.Close()
	if err != nil {
		log.Fatalln(err)
	}
}
