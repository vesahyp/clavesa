package observability

import "testing"

func TestTrimSparkStackTrace(t *testing.T) {
	t.Run("table-not-found keeps message, drops frames", func(t *testing.T) {
		in := "[TABLE_OR_VIEW_NOT_FOUND] The table or view `db`.`nope` cannot be found. SQLSTATE: 42P01; line 1 pos 14;\n" +
			"\tat org.apache.spark.sql.catalyst.analysis.package$.fail(package.scala:52)\n" +
			"\tat org.apache.spark.sql.connect.service.SessionHolder.withSession(SessionHolder.scala:340)\n"
		got := trimSparkStackTrace(in)
		want := "[TABLE_OR_VIEW_NOT_FOUND] The table or view `db`.`nope` cannot be found. SQLSTATE: 42P01; line 1 pos 14;"
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	})

	t.Run("parse error keeps the == SQL == caret block", func(t *testing.T) {
		in := "[PARSE_SYNTAX_ERROR] Syntax error at or near '('. SQLSTATE: 42601 (line 1, pos 25)\n\n" +
			"== SQL ==\n" +
			"SELECT payment_type COUNT(*) FROM trips\n" +
			"-------------------------^^^\n" +
			"\tat org.apache.spark.sql.parser.parse(ParseDriver.scala:100)\n"
		got := trimSparkStackTrace(in)
		if want := "-------------------------^^^"; got[len(got)-len(want):] != want {
			t.Errorf("caret block not preserved; got tail %q", got)
		}
	})

	t.Run("cuts at JVM stacktrace header (analysis error)", func(t *testing.T) {
		in := "[TABLE_OR_VIEW_NOT_FOUND] cannot be found. SQLSTATE: 42P01; line 1 pos 14;\n" +
			"'GlobalLimit 1\n+- 'UnresolvedRelation [db, nope]\n\n" +
			"JVM stacktrace:\n" +
			"org.apache.spark.sql.catalyst.ExtendedAnalysisException\n" +
			"\tat org.apache.spark.sql.catalyst.analysis.fail(package.scala:52)\n"
		got := trimSparkStackTrace(in)
		if want := "[TABLE_OR_VIEW_NOT_FOUND] cannot be found. SQLSTATE: 42P01; line 1 pos 14;"; got[:len(want)] != want {
			t.Errorf("message head not preserved; got %q", got)
		}
		if contains(got, "JVM stacktrace") || contains(got, "ExtendedAnalysisException") {
			t.Errorf("stack header/class leaked through: %q", got)
		}
	})

	t.Run("no frames returned as-is", func(t *testing.T) {
		in := "some plain message"
		if got := trimSparkStackTrace(in); got != "some plain message" {
			t.Errorf("got %q", got)
		}
	})
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
