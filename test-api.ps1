$base = "http://localhost:8080"

Write-Host "=== PUT foo=bar ==="
Invoke-RestMethod -Uri "$base/kv/foo" -Method Put -Body "bar"

Write-Host "=== PUT hello=world ==="
Invoke-RestMethod -Uri "$base/kv/hello" -Method Put -Body "world"

Write-Host "=== GET foo ==="
$r = Invoke-RestMethod -Uri "$base/kv/foo" -Method Get
if ($r -ne "bar") { throw "expected 'bar', got '$r'" }
Write-Host $r

Write-Host "=== GET hello ==="
$r = Invoke-RestMethod -Uri "$base/kv/hello" -Method Get
if ($r -ne "world") { throw "expected 'world', got '$r'" }
Write-Host $r

Write-Host "=== PUT foo=updated (overwrite) ==="
Invoke-RestMethod -Uri "$base/kv/foo" -Method Put -Body "updated"

Write-Host "=== GET foo (after update) ==="
$r = Invoke-RestMethod -Uri "$base/kv/foo" -Method Get
if ($r -ne "updated") { throw "expected 'updated', got '$r'" }
Write-Host $r

Write-Host "=== DELETE hello ==="
Invoke-RestMethod -Uri "$base/kv/hello" -Method Delete

Write-Host "=== GET hello (expect 404) ==="
try {
    Invoke-RestMethod -Uri "$base/kv/hello" -Method Get
    throw "expected 404 but got success"
} catch {
    if ($_.Exception.Response.StatusCode.value__ -eq 404) {
        Write-Host "404 (correct)"
    } else {
        throw $_
    }
}

Write-Host "=== POST /compact ==="
Invoke-RestMethod -Uri "$base/compact" -Method Post

Write-Host "=== GET foo (after compact) ==="
$r = Invoke-RestMethod -Uri "$base/kv/foo" -Method Get
if ($r -ne "updated") { throw "expected 'updated', got '$r'" }
Write-Host $r

Write-Host "=== ALL TESTS PASSED ==="
