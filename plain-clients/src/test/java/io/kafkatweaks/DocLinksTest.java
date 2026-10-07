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
 * Keeps the docs navigable: every relative link in README.md and docs/ points at a file that exists, every chapter has
 * the parts a reader and a lector rely on (header card, key takeaways, nav footer, and lector notes where the private
 * docs/teaching/ kit is checked out), and the chapter
 * files the two demo dispatchers print ({@code Run}, the Spring {@code Catalogue}) exist.
 */
class DocLinksTest {

    private static final Path REPO = Path.of("..").toAbsolutePath().normalize();
    private static final Path TEACHING = REPO.resolve("docs/teaching");
    private static final Pattern LINK = Pattern.compile("\\[[^\\]]*]\\(([^)\\s]+)\\)");
    private static final Pattern FENCE = Pattern.compile("(?ms)^\\s*```.*?^\\s*```");
    private static final Pattern CHAPTER = Pattern.compile("(\\d\\d)-[a-z0-9-]+\\.md");
    private static final Pattern CATALOGUE_DOC = Pattern.compile("\"(\\d\\d-[a-z0-9-]+\\.md)\"");

    @Test
    void everyRelativeLinkResolves() throws IOException {
        var problems = new ArrayList<String>();
        for (Path doc : markdownFiles()) {
            String text = FENCE.matcher(Files.readString(doc)).replaceAll("");
            Matcher m = LINK.matcher(text);
            while (m.find()) {
                String target = m.group(1);
                if (target.matches("^[a-z]+:.*") || target.startsWith("#")) {
                    continue;   // http(s), mailto, same-page anchors
                }
                String file = target.replaceFirst("#.*$", "");
                if (!Files.exists(doc.getParent().resolve(file).normalize())) {
                    problems.add("%s: %s".formatted(REPO.relativize(doc), target));
                }
            }
        }
        assertThat(problems).as("broken relative links").isEmpty();
    }

    @Test
    void everyChapterHasItsScaffolding() throws IOException {
        var problems = new ArrayList<String>();
        for (Path chapter : chapters()) {
            String name = chapter.getFileName().toString();
            String text = Files.readString(chapter).replace("\r\n", "\n");   // a Windows checkout has CRLF line endings
            if (!name.startsWith("00-")) {
                if (!text.contains("> **Level:**")) {
                    problems.add(name + ": no header card (> **Level:** ...)");
                }
                if (!text.contains("\n## Key takeaways\n")) {
                    problems.add(name + ": no '## Key takeaways' section");
                }
                // The lector kit lives on a private branch only; where it is checked out, every chapter needs its notes.
                if (Files.isDirectory(TEACHING) && !Files.exists(TEACHING.resolve("notes").resolve(name))) {
                    problems.add(name + ": no lector notes in docs/teaching/notes/");
                }
            }
            if (!text.contains("[Index](README.md)")) {
                problems.add(name + ": no nav footer ([Index](README.md))");
            }
        }
        assertThat(problems).isEmpty();
    }

    @Test
    void theDispatchersPointAtExistingChapters() throws IOException {
        var docs = new ArrayList<String>();
        Run.all().forEach(e -> docs.add(e.doc()));
        Matcher m = CATALOGUE_DOC.matcher(Files.readString(
                REPO.resolve("spring-boot-kafka/src/main/java/io/kafkatweaks/spring/Catalogue.java")));
        while (m.find()) {
            docs.add(m.group(1));
        }
        assertThat(docs).hasSize(21);
        assertThat(docs).allSatisfy(doc -> assertThat(REPO.resolve("docs").resolve(doc)).exists());
    }

    private static List<Path> markdownFiles() throws IOException {
        var out = new ArrayList<Path>();
        out.add(REPO.resolve("README.md"));
        try (Stream<Path> docs = Files.walk(REPO.resolve("docs"))) {
            docs.filter(p -> p.toString().endsWith(".md")).sorted().forEach(out::add);
        }
        return out;
    }

    private static List<Path> chapters() throws IOException {
        try (Stream<Path> docs = Files.list(REPO.resolve("docs"))) {
            return docs.filter(p -> CHAPTER.matcher(p.getFileName().toString()).matches()).sorted().toList();
        }
    }
}
