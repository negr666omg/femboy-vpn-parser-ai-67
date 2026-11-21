package app

import "testing"

func TestParseCursorPosition(t *testing.T) {
	row, ok := parseCursorPosition("18;73")
	if !ok || row != 18 {
		t.Fatalf("parseCursorPosition() = %d, %v; want row 18", row, ok)
	}
	if _, ok := parseCursorPosition("?18;73"); ok {
		t.Fatal("private cursor response should not be accepted as a normal cursor position")
	}
}

func TestParseSGRMouseSequence(t *testing.T) {
	button, col, row, pressed, ok := parseSGRMouseSequence("<0;27;11", 'M')
	if !ok || !pressed || button != 0 || col != 27 || row != 11 {
		t.Fatalf("left click parsed as button=%d col=%d row=%d pressed=%v ok=%v", button, col, row, pressed, ok)
	}
	_, _, _, pressed, ok = parseSGRMouseSequence("<0;27;11", 'm')
	if !ok || pressed {
		t.Fatalf("mouse release must not be treated as a click: pressed=%v ok=%v", pressed, ok)
	}
	_, _, _, _, ok = parseSGRMouseSequence("<2;27;11", 'M')
	if !ok {
		t.Fatal("right-click event should parse even though the handler ignores it")
	}
}

func TestLiveTitleClickRange(t *testing.T) {
	line := "[PARSE 2/12 ok:1 skip:0] | https://sub.example/group/nl?token=REDACTED | downloading"
	start, end, ok := liveTitleClickRange(line, "[PARSE 2/12 ok:1 skip:0]")
	if !ok || start < 1 || end <= start {
		t.Fatalf("invalid click range: %d-%d, ok=%v", start, end, ok)
	}
	prefix := "[PARSE 2/12 ok:1 skip:0] | "
	if start != displayWidth(prefix)+1 {
		t.Fatalf("click range starts at %d, want %d", start, displayWidth(prefix)+1)
	}
}
