# Generate one narration WAV per storyboard scene with the offline Windows voice (System.Speech).
param(
  [string]$Storyboard = "$PSScriptRoot\storyboard.json",
  [string]$OutDir = "$PSScriptRoot\build\audio",
  [string]$Voice = "Microsoft Zira Desktop",
  [int]$Rate = 0
)
Add-Type -AssemblyName System.Speech
New-Item -ItemType Directory -Force $OutDir | Out-Null
$scenes = Get-Content $Storyboard -Raw -Encoding UTF8 | ConvertFrom-Json
foreach ($s in $scenes) {
  $synth = New-Object System.Speech.Synthesis.SpeechSynthesizer
  $synth.SelectVoice($Voice)
  $synth.Rate = $Rate
  $fmt = New-Object System.Speech.AudioFormat.SpeechAudioFormatInfo(22050, [System.Speech.AudioFormat.AudioBitsPerSample]::Sixteen, [System.Speech.AudioFormat.AudioChannel]::Mono)
  $synth.SetOutputToWaveFile((Join-Path $OutDir "$($s.id).wav"), $fmt)
  $synth.Speak($s.narration)
  $synth.Dispose()
  Write-Output "narrated $($s.id)"
}
