# vhs tape for s3du's README demo.gif.
#
# Render with:
#   brew install vhs ffmpeg ttyd
#   go run ./internal/cmd/gen-demo
#   vhs demo.tape
#
# Output: demo.gif (~1.5 MB at 800x500). Replaces the old screenshot.png.

Output demo.gif

Set Theme "Catppuccin Mocha"
Set FontSize 14
Set Width 1100
Set Height 600
Set Padding 12
Set TypingSpeed 60ms
Set PlaybackSpeed 1.0

# Show the launch command so the viewer sees what's running.
Type "s3du -load -snapshot /tmp/s3du-demo.snap -i"
Sleep 600ms
Enter

# Initial listing — default sort (size descending, dirs first).
Sleep 2s

# Cycle bar widget: off → bar → bar+% → % only → off (back to default).
Type "g"
Sleep 1.5s
Type "g"
Sleep 1.5s
Type "g"
Sleep 1.5s
Type "g"
Sleep 1s

# Sort by name, then back to size.
Type "n"
Sleep 1.5s
Type "s"
Sleep 1.5s

# Sort by monthly storage cost — usually flips the order.
Type "$"
Sleep 1.5s

# Descend into the heaviest dir, scroll a bit, ascend.
Enter
Sleep 1.5s
Down
Sleep 300ms
Down
Sleep 300ms
Down
Sleep 600ms
Backspace
Sleep 1s

# Toggle directories-first off and back on.
Type "t"
Sleep 1.2s
Type "t"
Sleep 800ms

# Pop the help modal, then dismiss with any key.
Type "?"
Sleep 2.5s
Space
Sleep 600ms

# Exit cleanly.
Type "q"
Sleep 500ms
