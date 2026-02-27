<?php

declare(strict_types=1);

namespace App;

use App\Traits\WriterTokenizedV1Trait;
use App\Traits\SetupTokenizedV1Trait;
use App\Traits\LoaderTokenizedSocketV1Trait;
use App\Traits\LoaderTokenizedSocketV1iTrait;
use App\Traits\LoaderTokenizedShmopV1;

final class Parser
{
    private ?string $inputPath = null;
    private ?string $outputPath = null;

    // Schema parsing configurations
    const int DOMAIN_LENGTH     = 25;   // prefix chars to strip from URL field
                                        // "https://stitcher.io/blog/"
    const int DATE_LENGTH       = 10;   // "2026-01-01"
    const int DATE_WIDTH        = 25;   // Full datetime column width inc. time component

    // Tuning configurations
    const int WORKER_COUNT      = 8;                    // Should match physical core count
    const int WRITE_BUFFER      = 128 * 1024;           // 128kb output write buffer
    const int PRESCAN_BUFFER    = 256 * 1024;           // 256kb - enough to see all 269 urls
    const int READ_BUFFER       = 64 * 1024 * 1024;     // 64mb - Bumping up since we have 12gb of memory available

    // Token Tables
    private array $urlPool              = [];   // url_string -> true
    private array $urlTokens            = [];   // url_string -> int token
    private array $dateTokens           = [];   // packed_int -> int token
    private array $dateStrTokens        = [];   // date_string -> int token
    private array $dateChars            = [];   // date_string -> 2 byte packed char
    private array $urlStrings           = [];   // int token -> url_string
    private array $dateStrings          = [];   // int token -> date_string "2026-01-01"
    private array $urlJsonKeys          = [];
    private array $dateJsonPrefixes     = [];
    private int $urlCount               = 0;
    private int $dateCount              = 0;
    private int $minUrlLength           = 999;
    private int $minLineLength          = 35;

    // Tokenized Socket Implementation
    use SetupTokenizedV1Trait;
    use LoaderTokenizedSocketV1Trait;
    use WriterTokenizedV1Trait;

    // Tokenized SHMOP Implementation
//    use SetupTokenizedV1Trait;
//    use LoaderTokenizedShmopV1;
//    use WriterTokenizedV1Trait;

    /**
     * This is the main data structure that is populated by the load
     * process and then written to the output file by the writer
     */
    private array $data = [];

    public function __construct()
    {
        // Eliminate GC pauses mid-parse
        gc_disable();
    }

    public function parse(string $inputPath, string $outputPath): void
    {
        $this->inputPath = $inputPath;
        $this->outputPath = $outputPath;

        // -- Phase 0: Setup ----
        $this->setup();

        // -- Phase 1: Load the data ----
        $this->data = $this->load();

        // -- Phase 3: Write Output ----
        $this->write();
    }

}