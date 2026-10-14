param([string]$DeckPath)
$ErrorActionPreference = 'Stop'
$buildRoot = Split-Path -Parent $PSCommandPath
$previewRoot = Join-Path $buildRoot 'powerpoint_preview'
New-Item -ItemType Directory -Path $previewRoot -Force | Out-Null
$powerpoint = New-Object -ComObject PowerPoint.Application
$priorCount = $powerpoint.Presentations.Count
$deck = $null
$report = [System.Collections.Generic.List[object]]::new()
try {
    $deck = $powerpoint.Presentations.Open($DeckPath, -1, 0, 0)
    for ($i = 1; $i -le $deck.Slides.Count; $i++) {
        $slide = $deck.Slides.Item($i)
        $imagePath = Join-Path $previewRoot ('slide-{0:D2}.png' -f $i)
        $slide.Export($imagePath, 'PNG', 1920, 1080)
        $chartCount = 0
        $tableCount = 0
        $overflows = [System.Collections.Generic.List[object]]::new()
        foreach ($shape in $slide.Shapes) {
            if ($shape.HasChart -eq -1) { $chartCount++ }
            if ($shape.HasTable -eq -1) { $tableCount++ }
            if ($shape.HasTextFrame -eq -1 -and $shape.TextFrame2.HasText -eq -1) {
                if ($shape.TextFrame2.TextRange.BoundHeight -gt ($shape.Height + 2)) {
                    $overflows.Add(@{shape=$shape.Name; text=$shape.TextFrame2.TextRange.Text; height=$shape.Height; textHeight=$shape.TextFrame2.TextRange.BoundHeight})
                }
            }
        }
        $report.Add(@{slide=$i; charts=$chartCount; tables=$tableCount; textOverflows=@($overflows.ToArray()); image=$imagePath})
    }
    $deck.Close()
    $deck = $null
    $copyPath = Join-Path $buildRoot 'powerpoint_roundtrip.pptx'
    Copy-Item -LiteralPath $DeckPath -Destination $copyPath -Force
    $deck = $powerpoint.Presentations.Open($copyPath, 0, 0, 0)
    $editedChart = $false
    foreach ($shape in $deck.Slides.Item(2).Shapes) {
        if ($shape.HasChart -eq -1) {
            $originalWidth = $shape.Width
            $shape.Width = $originalWidth + 1
            $shape.Width = [double]$originalWidth
            $editedChart = $true
            break
        }
    }
    $deck.Save()
    $deck.Close()
    $deck = $null
    $deck = $powerpoint.Presentations.Open($copyPath, -1, 0, 0)
    $reopenedSlides = $deck.Slides.Count
    $deck.Close()
    $deck = $null
    @{application='Microsoft PowerPoint'; version=$powerpoint.Version; slides=$report.ToArray(); roundtrip=@{chartResizeSucceeded=$editedChart; reopenedSlideCount=$reopenedSlides; path=$copyPath}} | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $buildRoot 'powerpoint_validation.json') -Encoding utf8
    Write-Output "Rendered $($report.Count) slides in PowerPoint; roundtrip reopened $reopenedSlides slides."
} finally {
    if ($null -ne $deck) { $deck.Close() }
    if ($priorCount -eq 0) { $powerpoint.Quit() }
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($powerpoint) | Out-Null
}
