package main

import (
  _ "gopkg.in/cq.v1"
  "database/sql"
  "compress/gzip"
  "bufio"
  "fmt"
  "log"
  "os"
  "regexp"
  "strings"
  "flag"
)

func article_loader(lines <-chan string, neo4j_conn* string) {
  db, err := sql.Open("neo4j-cypher", *neo4j_conn)
  if err != nil {
    log.Println("error connecting to neo4j:", err)
  }
  defer db.Close()
  stmt_insert_page, err := db.Prepare(`CREATE (n:article {title:{0}, id:{1}})`)

  if err != nil {
    log.Fatal(err)
  }
  defer stmt_insert_page.Close()

  re := regexp.MustCompile("^([0-9]+),0,'([^']+)','")

  for l := range lines {
    //example line:
    //2,0,'Armonium','',0,0,0,0.42927655132,'20160331135152','20160331125058',78252459,8131,0,'wikitext'
    //Go CSV parser does not support arbitrary text delimiters, and the regex to escape them use gobbling and it's a bit slow, this ugly hack avoids that
    t := strings.Replace(l, "\\'", "XHIWNDKAODQ", -1)
    sm := re.FindStringSubmatch(t)
    if len(sm) == 0 {
      continue
    }

    _, err := stmt_insert_page.Query(strings.Replace(sm[2], "XHIWNDKAODQ", "\\'", -1), sm[1])
    if err != nil {
      log.Fatal(err)
    }
  }
}



func main() {
  neo4j_conn := flag.String("neo4j_conn", "http://localhost:7474", "Neo4j connection string")
  pages_file := flag.String("pages_file", "enwiki-20160407-page.sql.gz", "compressed SQL file with the pages")
  links_file := flag.String("pages_file", "enwiki-20160407-pagelinks.sql.gz", "compressed SQL file with the page links")

  flag.Parse()

  f, err := os.Open(*pages_file)
  if err != nil {
    log.Fatal(err)
  }
  defer f.Close()
  gr, err := gzip.NewReader(f)
  if err != nil {
    log.Fatal(err)
  }
  defer gr.Close()

  // Define a split function that separates on SQL tuple separatore.
  onComma := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
    if len(data)<4 && !atEOF {
      return 0, nil, nil
    }
    for i := 0; i < len(data) - 3; i++ {
      if data[i] == ')' && data[i+1] == ',' && data[i+2] == '(' {
        return i + 3, data[:i], nil
      }
    }
    //nothing found, ask for more
    return 0, nil, nil
  }
  scanner := bufio.NewScanner(gr)
  scanner.Split(onComma)

  lines := make(chan string, 100)

  for x := 1; x <= 10; x++ {
    go article_loader(lines, neo4j_conn)
  }

  fmt.Println("step 1 of 2: loading the article nodes (id and title)")
  processed := 0
  for scanner.Scan() {
    lines <- scanner.Text()
    processed++
    if processed % 1000 == 0 {
      fmt.Printf(" -- imported %d article nodes\n", processed)
    }
  }

  close(lines)

  fmt.Println("finished to load page titles, now loading the link relationships")

}
