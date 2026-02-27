<?php

namespace App\Traits;

use function substr;
use function strpos;
use function strlen;
use const SEEK_CUR;

trait LoaderTokenizedShmopV1 {
    private function load(): array
    {
        $chunks = $this->calculateChunkBoundaries();
        $workerCount = count($chunks);  // Derived from self::WORKER_COUNT

        // -- Setup shared memory ----

        // Round up to nearest 128 bytes to match M1 cache line size
        $rawSlice = $this->urlCount * $this->dateCount * 4;
        $sliceSize = (int) ceil($rawSlice/128) * 128;

        $totalSize = $workerCount * $sliceSize;
        $shmKey = ftok(__FILE__, 'p');

        $shm = shmop_open($shmKey, 'c', 0600, $totalSize);
        if ($shm === false) {
            throw new \RuntimeException("shmop_open failed");
        }

        // Fork workers
        $pids = [];
        foreach ($chunks as $index => [$start, $end]) {
            $pid = pcntl_fork();

            if ($pid === -1) {
                throw new \RuntimeException("pcntl_fork failed for worker $index");
            }

            if ($pid === 0) {
                // Child process
                $this->processChunk($start, $end, $index, $shm, $sliceSize);
                exit(0);
            }

            // Parent process
            $pids[$index] = $pid;
        }

        // All forks closed
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $accumulator = $this->makeAccumulator();

        for ($i = 0; $i < $workerCount; $i++) {
            $j = 0;
            foreach (unpack('V*', shmop_read($shm, $i * $sliceSize, $rawSlice)) as $v) {
                $accumulator[$j++] += $v;
            }
        }

        shmop_delete($shm);

        return $accumulator;
    }

    private function makeAccumulator(): array
    {
        return array_fill(0, $this->urlCount * $this->dateCount, 0);
    }

    private function calculateChunkBoundaries(): array
    {
        $fileSize = filesize($this->inputPath);
        $chunkSize = (int)ceil($fileSize / self::WORKER_COUNT);
        $chunks = [];
        $start = 0;
        $handle = fopen($this->inputPath, 'rb', false);

        for ($i = 0; $i < self::WORKER_COUNT; $i++) {
            if ($start >= $fileSize) break;

            if ($i === self::WORKER_COUNT - 1) {
                $chunks[] = [$start, $fileSize];
                break;
            }

            $end = min($start + $chunkSize, $fileSize);

            if ($end < $fileSize) {
                fseek($handle, $end);
                fgets($handle);
                $end = ftell($handle);
            }

            $chunks[] = [$start, $end];
            $start = $end;
        }

        fclose($handle);

        return $chunks;
    }

    private function processChunk(int $start, int $end, int $index, \Shmop $shm, int $sliceSize): void
    {
        $buckets = array_fill(0, $this->urlCount, '');

        $handle = fopen($this->inputPath, 'rb', false);
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $totalRowCount = 0;
        $tReadTotal = 0;

        // Hoist these properties to prevent Zend engine hash lookups from $this within the hot path
        $urlTokens  = $this->urlTokens;
        $dateChars  = $this->dateChars;
        $minLineLen = $this->minLineLength;

        $remaining = $end - $start;

        while ($remaining > 0) {

            // -- Read one window ----
            $toRead = min($remaining, self::READ_BUFFER);
            $window = fread($handle, $toRead);

            if ($window === false || $window === '') break;

            $windowLen = strlen($window);
            $lastNl = strrpos($window, "\n");

            if ($lastNl === false) {
                $remaining -= $windowLen;
                continue;
            }

            $tail = $windowLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
            }

            $remaining -= ($windowLen - $tail);
            $windowEnd = $lastNl;
            $wStart = 0;

            // 5x Unrolled fast path
            $fence = $windowEnd - 600;

            while ($wStart < $fence) {
                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $totalRowCount += 5;
            }

            // -- Cleanup loop for rows after the fence ----
            while ($wStart < $windowEnd) {
                $wEnd = strpos($window, "\n", $wStart + $minLineLen);

                if ($wEnd === false || $wEnd > $windowEnd) break;

                $urlToken = $urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? null;
                $dateChar = $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? null;

                if ($urlToken !== null && $dateChar !== null) {
                    $buckets[$urlToken] .= $dateChar;
                }

                $wStart = $wEnd + 1;
                $totalRowCount++;
            }
        }

        fclose($handle);

        // Convert buckets to flat counts array
        $counts = array_fill(0, $this->urlCount * $this->dateCount, 0);

        for ($s = 0; $s < $this->urlCount; $s++) {
            if ($buckets[$s] === '') continue;
            $base = $s * $this->dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$s])) as $dateId => $count) {
                $counts[$base + $dateId] = $count;
            }
        }

        // Send flat counts to parent via shmop
        shmop_write($shm, pack('V*', ...$counts), $index * $sliceSize);
    }
}