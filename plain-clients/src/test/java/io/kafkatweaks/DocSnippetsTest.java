package io.kafkatweaks;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Keeps the chapters' "The code that matters" snippets true to the code. Every fenced block in docs/*.md that follows a
 * {@code <!-- recipe: <repo path> -->} marker must be a verbatim excerpt of that file: each non-blank snippet line
 * (whitespace-trimmed) must appear in the file, in the same order. A line {@code // ...} or {@code # ...} marks an
 * omission and is skipped.
 */
class DocSnippetsTest {

    private static final Path REPO = Path.of("..").toAbsolutePath().normalize();
    private static final Pattern SNIPPET = Pattern.compile("<!-- recipe: (\\S+) -->\\s*```\\w*\\R(.*?)\\R```", Pattern.DOTALL);

    record Snippet(Path doc, String source, List<String> lines) {
    }

    @Test
    void everySnippetIsAnExcerptOfItsRecipeFile() throws IOException {
        List<Snippet> snippets = snippets();
        assertThat(snippets).as("snippets found in docs/").hasSizeGreaterThan(20);
        var problems = new ArrayList<String>();
        for (Snippet s : snippets) {
            Path source = REPO.resolve(s.source());
            if (!Files.exists(source)) {
                problems.add("%s: %s does not exist".formatted(s.doc().getFileName(), s.source()));
                continue;
            }
            List<String> file = Files.readAllLines(source).stream().map(String::strip).toList();
            int at = -1;
            for (String line : s.lines()) {
                int found = indexOf(file, line, at + 1);
                if (found < 0) {
                    problems.add("%s -> %s: not found (in order): %s".formatted(s.doc().getFileName(), s.source(), line));
                    break;
                }
                at = found;
            }
        }
        assertThat(problems).isEmpty();
    }

    private static int indexOf(List<String> file, String line, int from) {
        for (int i = from; i < file.size(); i++) {
            if (file.get(i).equals(line)) {
                return i;
            }
        }
        return -1;
    }

    private static List<Snippet> snippets() throws IOException {
        var out = new ArrayList<Snippet>();
        try (Stream<Path> docs = Files.list(REPO.resolve("docs"))) {
            for (Path doc : docs.filter(p -> p.toString().endsWith(".md")).sorted().toList()) {
                Matcher m = SNIPPET.matcher(Files.readString(doc));
                while (m.find()) {
                    List<String> lines = m.group(2).lines().map(String::strip)
                            .filter(l -> !l.isEmpty() && !l.equals("// ...") && !l.equals("# ..."))
                            .toList();
                    out.add(new Snippet(doc, m.group(1), lines));
                }
            }
        }
        return out;
    }
}
