<?php

namespace App\Traits;

trait WorkerTokenizedSocketV2Trait {
    private function work(int $start, int $end, int $index, $writeSocket = null): array
    {
        $buckets = array_fill(0, $this->urlCount, '');

        // Hoist these properties to prevent Zend engine hash lookups from $this within the hot path
        $urlTokens  = $this->urlTokens;
        $dateChars  = $this->dateChars;
        $minLineLen = $this->minLineLength;

        $handle = fopen($this->inputPath, 'rb', false);
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $totalRowCount = 0;
        $tReadTotal = 0;

        $fastPathRows  = 0;
        $cleanupRows   = 0;
        $minRowLen     = PHP_INT_MAX;
        $maxRowLen     = 0;
        $totalRowBytes = 0;
        $rowCount      = 0;

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
            //$fence = $windowEnd - 600;
            $fence = $windowEnd - ((7 * 99) + 8);

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

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                $wEnd = strpos($window, "\n", $wStart + $minLineLen);
                $buckets[$urlTokens[substr($window, $wStart + self::DOMAIN_LENGTH, $wEnd - $wStart - self::DOMAIN_LENGTH - self::DATE_WIDTH - 1)] ?? -1]
                    .= $dateChars[substr($window, $wEnd - self::DATE_WIDTH, self::DATE_LENGTH)] ?? '';
                $wStart = $wEnd + 1;

                if (($fastPathRows % 5000) === 0) {
                    $rowLen = $wEnd - $wStart + 5; // approximate
                    if ($rowLen > $maxRowLen) $maxRowLen = $rowLen;
                    $totalRowBytes += $rowLen * 5;
                    $rowCount += 5;
                }

                $fastPathRows += 5;
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

                $rowLen = $wEnd - $wStart;
                if ($rowLen < $minRowLen) $minRowLen = $rowLen;
                if ($rowLen > $maxRowLen) $maxRowLen = $rowLen;
                $totalRowBytes += $rowLen;
                $rowCount++;
                $cleanupRows++;

                $wStart = $wEnd + 1;
            }
        }

        $avgRowLen = $rowCount > 0 ? $totalRowBytes / $rowCount : 0;
        $totalRows = $fastPathRows + $cleanupRows;
        $fastPct   = $totalRows > 0 ? ($fastPathRows / $totalRows) * 100 : 0;

        fprintf(STDERR, "[worker %d] rows=%d fast=%.1f%% cleanup=%d min=%d max=%d avg=%.1f\n",
            $index, $totalRows, $fastPct, $cleanupRows, $minRowLen, $maxRowLen, $avgRowLen);

        fclose($handle);

        // Convert buckets to flat counts array
        $counts = array_fill(0, $this->urlCount * $this->dateCount, 0);
        $maxVal = 0;

        for ($s = 0; $s < $this->urlCount; $s++) {
            if ($buckets[$s] === '') continue;
            $base = $s * $this->dateCount;
            foreach (array_count_values(unpack('v*', $buckets[$s])) as $dateId => $count) {
                $counts[$base + $dateId] = $count;
                if ($count > $maxVal) $maxVal = $count;
            }
        }

        // Send flat counts to parent via socket
        //$maxVal = count($counts) > 0 ? max($counts) : 0;
        $v16 = $maxVal <= 65535;

        if ($writeSocket !== null) {
            fwrite($writeSocket, $v16 ? "\x00" : "\x01");
            $fmt = $v16 ? 'v*' : 'V*';
            fwrite($writeSocket, pack($fmt, ...$counts));
        }

        return $counts;
    }
}