package committee

import (
	"blockEmulator/params"
	"blockEmulator/partition"
	"fmt"
	"os"
)

func writePartition(epoch int32, fileName string, partitionMap map[partition.Vertex]int) error {
	partitionFilePath := params.ExpDataRootDir + "/" + fileName

	partitionFile, err := os.OpenFile(partitionFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", partitionFilePath, err)
	}
	defer partitionFile.Close()

	if _, err := partitionFile.WriteString(fmt.Sprintf("Epoch: %d\n", epoch)); err != nil {
		return fmt.Errorf("failed to write epoch to %s: %w", partitionFilePath, err)
	}

	for key, value := range partitionMap {
		line := fmt.Sprintf("%s: %d\n", key.Addr, value)
		if _, err := partitionFile.WriteString(line); err != nil {
			return fmt.Errorf("failed to write line to %s: %w", partitionFilePath, err)
		}
	}

	return nil
}

func writeVertexSet(epoch int32, fileName string, vertexSet map[partition.Vertex]bool) error {
	vertexSetFilePath := params.ExpDataRootDir + "/" + fileName

	vertexSetFile, err := os.OpenFile(vertexSetFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", vertexSetFilePath, err)
	}
	defer vertexSetFile.Close()

	if _, err := vertexSetFile.WriteString(fmt.Sprintf("Epoch: %d\n", epoch)); err != nil {
		return fmt.Errorf("failed to write epoch to %s: %w", vertexSetFilePath, err)
	}

	for key, value := range vertexSet {
		line := fmt.Sprintf("%s: %t\n", key.Addr, value)
		if _, err := vertexSetFile.WriteString(line); err != nil {
			return fmt.Errorf("failed to write to %s: %w", vertexSetFilePath, err)
		}
	}

	return nil
}

func writeMergedContracts(epoch int32, fileName string, mergedContracts map[string]partition.Vertex) error {
	mergedContractsFilePath := params.ExpDataRootDir + "/" + fileName

	mergedContractsFile, err := os.OpenFile(mergedContractsFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", mergedContractsFilePath, err)
	}
	defer mergedContractsFile.Close()

	if _, err := mergedContractsFile.WriteString(fmt.Sprintf("Epoch: %d\n", epoch)); err != nil {
		return fmt.Errorf("failed to write epoch to %s: %w", mergedContractsFilePath, err)
	}

	for key, vertex := range mergedContracts {
		line := fmt.Sprintf("%s: {Addr: %s}\n", key, vertex.Addr)
		if _, err := mergedContractsFile.WriteString(line); err != nil {
			return fmt.Errorf("failed to write to %s: %w", mergedContractsFilePath, err)
		}
	}

	return nil
}

func writeStringPartition(epoch int32, fileName string, partitionMap map[string]uint64) error {
	partitionFilePath := params.ExpDataRootDir + "/" + fileName

	partitionFile, err := os.OpenFile(partitionFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", partitionFilePath, err)
	}
	defer partitionFile.Close()

	if _, err := partitionFile.WriteString(fmt.Sprintf("Epoch: %d\n", epoch)); err != nil {
		return fmt.Errorf("failed to write epoch to %s: %w", partitionFilePath, err)
	}

	for key, value := range partitionMap {
		line := fmt.Sprintf("%s: %d\n", key, value)
		if _, err := partitionFile.WriteString(line); err != nil {
			return fmt.Errorf("failed to write line to %s: %w", partitionFilePath, err)
		}
	}

	return nil
}

func writeReversedContracts(fileName string, reversedMap map[partition.Vertex][]string) error {
	reversedContractsFilePath := params.ExpDataRootDir + "/" + fileName
	reversedContractsFile, err := os.OpenFile(reversedContractsFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", reversedContractsFilePath, err)
	}
	defer reversedContractsFile.Close()

	for vertex, addresses := range reversedMap {
		line := fmt.Sprintf("{Addr: %s}: %d\n", vertex.Addr, len(addresses))
		if _, err := reversedContractsFile.WriteString(line); err != nil {
			return fmt.Errorf("failed to write line to %s: %w", reversedContractsFilePath, err)
		}
	}

	return nil
}
