<?php
/**
 * Kroon-aanroep voor de Putoline-delta. Alleen vanaf de VPS draaien.
 *
 *   php motox_putoline_fetch.php assortment
 *   php motox_putoline_fetch.php download <url> <bestand>
 */

declare(strict_types=1);

const API_BASE = "https://api.kroon-oil.com";

function env_path(): string
{
    $candidates = [
        getenv("KROON_ENV") ?: "",
        "/home/motoxshop/.env.kroon",
        dirname(__DIR__, 2) . "/.env.kroon",
    ];
    foreach ($candidates as $path) {
        if ($path !== "" && is_file($path)) {
            return $path;
        }
    }
    fwrite(STDERR, "Geen .env.kroon\n");
    exit(1);
}

function load_env(string $path): array
{
    $env = [];
    foreach (file($path, FILE_IGNORE_NEW_LINES) as $line) {
        if ($line === "" || $line[0] === "#" || !str_contains($line, "=")) {
            continue;
        }
        [$key, $value] = explode("=", $line, 2);
        $env[$key] = $value;
    }
    if (empty($env["KROON_PUBLIC_KEY"]) || empty($env["KROON_PRIVATE_KEY"])) {
        fwrite(STDERR, "Kroon-sleutels ontbreken\n");
        exit(1);
    }
    return $env;
}

function api_raw(string $url, ?string $body, string $public, string $private): string
{
    $hash = hash_hmac("sha256", $body ?? "", $private);
    $ch = curl_init($url);
    $options = [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_TIMEOUT => 120,
        CURLOPT_HTTPHEADER => [
            "Content-type: application/json; charset=UTF-8",
            "X-Public: " . $public,
            "X-Hash: " . $hash,
        ],
    ];
    if ($body === null) {
        $options[CURLOPT_HTTPGET] = true;
    } else {
        $options[CURLOPT_POST] = true;
        $options[CURLOPT_POSTFIELDS] = $body;
    }
    curl_setopt_array($ch, $options);
    $raw = curl_exec($ch);
    $code = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);
    if ($raw === false || $code >= 400) {
        fwrite(STDERR, "Kroon HTTP $code $error\n");
        exit(1);
    }
    return $raw;
}

$env = load_env(env_path());
$command = $argv[1] ?? "";
$base = rtrim($env["KROON_API_BASE"] ?? API_BASE, "/");

if ($command === "assortment") {
    echo api_raw($base . "/assortment.json?lang=nl", "{}", $env["KROON_PUBLIC_KEY"], $env["KROON_PRIVATE_KEY"]);
    exit(0);
}

if ($command === "download") {
    $url = $argv[2] ?? "";
    $dest = $argv[3] ?? "";
    if ($url === "" || $dest === "") {
        fwrite(STDERR, "download verwacht url en bestand\n");
        exit(1);
    }
    $raw = api_raw($url, null, $env["KROON_PUBLIC_KEY"], $env["KROON_PRIVATE_KEY"]);
    if (file_put_contents($dest, $raw) === false) {
        fwrite(STDERR, "wegschrijven mislukt\n");
        exit(1);
    }
    exit(0);
}

fwrite(STDERR, "Onbekend commando\n");
exit(1);
