#!/usr/bin/env python
import sys
from typing import Optional, Dict

from rich import print
from rich.columns import Columns
from rich.console import Console
import typer

# fmt: off
# Mapping from topics to colors
TOPICS = {
    "COMMON": "white",
    "ELECT": "green",
    "TICKR": "#67a0b2",
    "HRTBT": "#d0b343",
    "APPND": "#70c43f",
    "BCMFL": "#4878bc",
    "START": "#398280",
    "APPLY": "#98719f",
    "MATCH": "#d08341",
    "SNAPS": "#FD971F",
    "ISNAP": "#fe2c79",
    "PRSST": "#ff615c",
    "CLNT": "#00813c",
    "INFO": "#ffffff",
    "WARN": "#d08341",
    "ERRO": "#fe2626",
    "TRCE": "#fe2626",
}
# fmt: on


def list_topics(value: Optional[str]):
    if value is None:
        return value
    topics = value.split(",")
    for topic in topics:
        if topic not in TOPICS:
            raise typer.BadParameter(f"topic {topic} not recognized")
    return topics


def main(
        file: typer.FileText = typer.Argument(
            None, help="File to read, stdin otherwise"),
        colorize: bool = typer.Option(True, "--no-color"),
        n_columns: Optional[int] = typer.Option(3, "--columns", "-c"),
        ignore: Optional[str] = typer.Option(
            None, "--ignore", "-i", callback=list_topics),
        just: Optional[str] = typer.Option(
            None, "--just", "-j", callback=list_topics),
        warning: bool = typer.Option(
            False, '--warning/--no-warning', '-w',
            help='Include messages with WRN'),
        with_code_line: bool = typer.Option(
            False, '--code-line/--no-code-line', '-cl',
            help='Show code line'),
        only_correlationID: Optional[str] = typer.Option(
            None, "--correlationid", "-cid"),):
    topics = list(TOPICS)

    # We can take input from a stdin (pipes) or from a file
    input_ = file if file else sys.stdin
    # Print just some topics or exclude some topics (good for avoiding verbose
    # ones)
    if just:
        topics = just
    if ignore:
        topics = [lvl for lvl in topics if lvl not in set(ignore)]

    topics = set(topics)
    console = Console(color_system="truecolor")
    width = console.size.width

    print("TOPICS:", end=" ")
    for topic in topics:
        color = TOPICS[topic]
        topic = f"[{color}]{topic}[/{color}]"
        console.print(topic, end=" ")
    print()

    panic = False
    for line in input_:
        try:
            if line.startswith("Test"):
                console.print(line, end="")
                console.print("#" * console.width)
                continue

            if not warning and "WRN" in line:
                continue

            # S5 ELECT 18:19:20.292541 raft.go:360: 6 peer voted against us
            peer, topic_cID, time, code_line, *msg = line.strip().split(" ")

            if len(peer) != 2:
                console.print(line, end="")

            # check that topic may has correlationID
            topic, *correlationID = topic_cID.strip().split("_")

            # time, topic, *msg = line.strip().split(" ")
            # To ignore some topics
            if topic not in topics:
                continue

            msg = " ".join(msg)

            if only_correlationID:
                if len(correlationID) == 0:
                    continue
                if only_correlationID != correlationID[0]:
                    continue

            if len(correlationID) == 1:
                msg = "[" + correlationID[0] + "] " + msg

            # Colorize output by using rich syntax when needed
            if colorize and topic in topics:
                color = TOPICS[topic]
                msg = f"[{color}]{msg}[/{color}]"

            if with_code_line:
                msg = msg + " (" + code_line[:-1] + ")"

            # Single column printing. Always the case for debug stmts in tests
            if n_columns is None or topic == "TEST":
                print(time, msg)
            # Multi column printing, timing is dropped to maximize horizontal
            # space. Heavylifting is done through rich.column.Columns object
            else:
                i = int(peer[1])  # peer = "S<N>"
                cols = ["" for _ in range(n_columns)]
                msg = "" + msg
                cols[i] = msg
                col_width = int(width / n_columns)
                cols = Columns(
                    cols,
                    width=col_width - 1,
                    equal=True,
                    expand=True)
                console.print(cols)
        except BaseException:
            # Code from tests or panics does not follow format
            # so we print it as is
            if line.startswith("panic"):
                panic = True
            # Output from tests is usually important so add a
            # horizontal line with hashes to make it more obvious
            if not panic:
                console.print("#" * console.width)
            console.print(line, end="")


if __name__ == "__main__":
    typer.run(main)
