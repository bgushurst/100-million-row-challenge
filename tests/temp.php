<?php

namespace Tests;

use App\Traits\LoaderLegacyTrait;
use App\Traits\SettingsMacMini;
use App\Traits\SetupSerializedTrait;
use App\Traits\WorkerLegacyTrait;
use App\Traits\WriterTokenizedV1Trait;

/**
 * Just some tests, nothing to see here
 */
class Temp {

    use SettingsMacMini;
    use SetupSerializedTrait;
    use LoaderLegacyTrait;
    use WorkerLegacyTrait;
    use WriterTokenizedV1Trait;

    public function __construct()
    {
        gc_disable();
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $this->inputPath = $inputPath;
        $this->outputPath = $outputPath;

        // -- Phase 0: Setup ----
        $t0 = hrtime(true);
        $this->setup();
        printf("Setup took %.2fms\n", (hrtime(true) - $t0) / 1e6);

        // -- Phase 1: Load the data ----
        $t0 = hrtime(true);
        $this->data = $this->load();
        printf("Load  took %.2fms\n", (hrtime(true) - $t0) / 1e6);

        // -- Phase 3: Write Output ----
        $t0 = hrtime(true);
        $this->write();
        printf("Write took %.2fms\n", (hrtime(true) - $t0) / 1e6);
    }

}