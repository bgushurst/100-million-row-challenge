<?php

namespace App\Traits;

trait WriterTokenizedV1Trait {

    private function write(): void
    {
        // Hoist any class variables
        $accumulator = $this->data;
        $outputPath = $this->outputPath;

        // Build json key caches
        if ($this->urlJsonKeys === []) {
            for ($i = 0; $i < $this->urlCount; $i++) {
                $this->urlJsonKeys[$i] = '    "\\/blog\\/' . str_replace('/', '\/', $this->urlStrings[$i]) . "\": {\n";
            }
        }

        if ($this->dateJsonPrefixes === []) {
            for ($i = 0; $i < $this->dateCount; $i++) {
                $this->dateJsonPrefixes[$i] = '        "' . $this->dateStrings[$i] . '": ';
            }
        }

        $lastActiveUrlToken = -1;
        for ($urlToken = $this->urlCount - 1; $urlToken >= 0; $urlToken--) {
            $base = $urlToken * $this->dateCount;
            for ($d = 0; $d < $this->dateCount; $d++) {
                if ($accumulator[$base + $d] !== 0) {
                    $lastActiveUrlToken = $urlToken;
                    break 2;
                }
            }
        }

        $out = fopen($outputPath, 'wb', false);
        stream_set_write_buffer($out, 0);

        $buf = "{\n";

        for ($urlToken = 0; $urlToken <= $this->urlCount - 1; $urlToken++) {
            $base = $urlToken * $this->dateCount;

            $activeDateCount = 0;
            for ($d = 0; $d < $this->dateCount; $d++) {
                if ($accumulator[$base + $d] !== 0) $activeDateCount++;
            }

            if ($activeDateCount === 0) continue;

            $isLastUrl = ($urlToken === $lastActiveUrlToken);

            $dateBuf = '';
            for ($d = 0; $d < $this->dateCount; $d++) {
                $count = $accumulator[$base + $d];
                if ($count === 0) continue;
                $dateBuf .= $this->dateJsonPrefixes[$d] . $count . ",\n";
            }
            if ($dateBuf === '') continue;

            $buf .= $this->urlJsonKeys[$urlToken]
                . substr($dateBuf, 0, -2)  . "\n"
                . ($isLastUrl ? "    }\n" : "    },\n");

            if (strlen($buf) >= self::WRITE_BUFFER) {
                fwrite($out, $buf);
                $buf = '';
            }
        }

        fwrite($out, $buf . '}');
        fclose($out);
    }

}