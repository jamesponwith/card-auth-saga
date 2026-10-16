# Renders docs/demo.gif from the real workflow: run `vhs docs/demo.tape` from the repo root.
# The story and its postings come from TestDemo (demo_test.go); this only films it.
Output docs/demo.gif

Set FontSize 20
Set Width 1120
Set Height 820
Set Padding 28
Set Theme "TokyoNight"
Set TypingSpeed 0

Hide
Type `go test -c -o "$TMPDIR/cas-demo.test" . && clear`
Enter
Sleep 25s
Type `clear && CAS_DEMO=1 CAS_DEMO_PACE=1 "$TMPDIR/cas-demo.test" -test.run TestDemo -test.v | grep --line-buffered -vE '^(=== RUN|--- PASS|PASS)'; rm -f "$TMPDIR/cas-demo.test"`
Enter
Sleep 300ms
Show
Sleep 15s
