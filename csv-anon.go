package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
)

var filename = flag.String("file", "", "A CSV file to parse")
var field = flag.String("field", "", "Name of the field to anonymise")

func anonymise(r *csv.Reader, field string) (valueMap map[string]int, err error) {
	s, err := r.Read() // first line is field names
	if err != nil {
		return nil, err
	}

	fieldIndex := -1 // the index of the field to be processed
	for pos, val := range s {
		if val == field {
			fieldIndex = pos
			break
		}
	}

	if fieldIndex == -1 {
		return nil, errors.New("No matching field found")
	}

	filename := "anonymised_data.csv"
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		return nil, errors.New("File anonymised_data.csv already exists. Aborting.")
	}

	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	outCsv := csv.NewWriter(file)
	defer outCsv.Flush()
	err = outCsv.Write(s) // write the headers back out
	if err != nil {
		return nil, err
	}

	valueMap = make(map[string]int)
	currentValueNumber := 0

	for s, err = r.Read(); err != io.EOF; s, err = r.Read() {
		if err != nil {
			return nil, err
		}

		value := s[fieldIndex]

		_, exists := valueMap[value]
		if !exists {
			currentValueNumber += 1
			valueMap[value] = currentValueNumber
		}

		s[fieldIndex] = field + "_" + fmt.Sprintf("%d", valueMap[value])
		err = outCsv.Write(s)
		if err != nil {
			return nil, err
		}
	}

	return valueMap, nil
}

func main() {
	flag.Parse()
	displayFields := false // just display the fields and exit

	if *filename == "" || *field == "" {
		if *filename == "" {
			fmt.Println("You must supply a filename")
			flag.Usage()
			os.Exit(1)
		} else {
			displayFields = true
		}
	}

	if _, err := os.Stat(*filename); os.IsNotExist(err) {
		log.Fatalf("File %v does not exist\n", *filename)
	}

	file, err := os.Open(*filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	r := csv.NewReader(file)
	if displayFields {
		fmt.Println("The available fields are:")
		s, err := r.Read()
		if err != nil {
			log.Fatal(err)
		}

		for _, field := range s {
			fmt.Printf("%v\n", field)
		}

		os.Exit(0)
	}

	// anonymising fields
	valueMap, err := anonymise(r, *field)
	if err != nil {
		log.Fatal(err)
	}

	// write the mapping to a file
	file, err = os.Create(*field + "_map.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	mapCsv := csv.NewWriter(file)
	defer mapCsv.Flush()
	err = mapCsv.Write([]string{"OriginalValue", "NewValue"})
	if err != nil {
		log.Fatal(err)
	}

	for key, val := range valueMap {
		var entry []string
		entry = make([]string, 2, 2)
		entry[0] = key
		entry[1] = *field + "_" + fmt.Sprintf("%d", val)
		err = mapCsv.Write(entry)
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Succesfully anonymised data")
}
