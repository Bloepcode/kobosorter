from typing import List
from uuid import uuid4, UUID
from datetime import datetime
import sqlite3
from sqlite3 import Connection
from shutil import rmtree, copyfile
import os
from rich import print
from dataclasses import dataclass, field


FILE_PREFIX = "file:///mnt/onboard/"
INTERACT_PATH = "interact"
DEFAULT_SHELF = "interact"
src_path = "/Volumes/KOBOeReader/"
db_path = os.path.join(src_path, ".kobo", "KoboReader.sqlite")
# db_path = ".kobo/KoboReader.sqlite"


@dataclass
class Book:
    path: str
    name: str
    shelf: str = None
    uuid: UUID = field(init=False, default_factory=uuid4)
    db_path: str = field(init=False)
    interacter_path: str = field(init=False)
    modified = False
    to_remove: str = None

    def __post_init__(self) -> None:
        self.db_path = f"{FILE_PREFIX}{self.name}"
        self.update()

    def update(self) -> None:
        self.interacter_path = os.path.join(
            INTERACT_PATH, self.shelf, self.name) + ".txt" if self.shelf != DEFAULT_SHELF else os.path.join(
            INTERACT_PATH, self.name) + ".txt"


def scan_books(cur):
    books = []
    shelfs = []
    for name in os.listdir(src_path):
        full_path = os.path.join(src_path, name)
        if os.path.isdir(full_path):
            continue
        if name[0] == ".":
            continue

        results = cur.execute(
            f'SELECT ShelfName FROM ShelfContent WHERE ContentId = (?)', (f"{FILE_PREFIX}{name}",)).fetchall()

        shelf = results[0][0] if len(results) != 0 else DEFAULT_SHELF
        if shelf and shelf not in shelfs:
            shelfs.append(shelf)

        books.append(
            Book(full_path, name, shelf))

    return books, shelfs


def place(books, shelfs: List[str]):
    for shelf in shelfs:
        if shelf == DEFAULT_SHELF:
            continue
        shelf_path = os.path.join(INTERACT_PATH, shelf)
        if os.path.exists(shelf_path):
            continue
        os.mkdir(shelf_path)

    for book in books:
        with open(book.interacter_path, "w") as f:
            f.write(str(book.uuid))


def handle_new_book(_file, path, shelf):
    file_path = os.path.join(path, _file)
    copyfile(file_path, os.path.join(src_path, _file))
    # os.remove(file_path)
    book = Book(file_path, _file, shelf)
    with open(file_path + ".txt", "w") as f:
        f.write(str(book.uuid))

    return book


def handle_renamed_book(original_book, shelf, new_name):
    new_name = os.path.splitext(new_name)[0]
    _src_path = os.path.join(
        src_path, new_name)
    os.rename(original_book.path, _src_path)
    book = Book(_src_path, new_name, shelf, to_remove=original_book.db_path)

    return book


def update_with_changes(books: List[Book]):
    shelfs = []
    for root, _, files in os.walk(INTERACT_PATH, topdown=False):
        shelf = os.path.split(root)[-1]
        if shelf != DEFAULT_SHELF and shelf not in shelfs:
            print(
                f"[cyan]Shelf found [blue]{shelf}[/blue].")
            shelfs.append(shelf)
        print(
            f"\n[cyan]Now going through shelf [blue]{shelf}[/blue]...[/cyan]")
        for _file in files:
            if _file[0] == ".":
                continue
            # Check for new ebook
            if os.path.splitext(_file)[1] == ".epub":
                print(
                    f"[cyan]New book found [blue]{_file}[/blue].")
                books.append(handle_new_book(_file, root, shelf))
                _file += ".txt"

            with open(os.path.join(root, _file), "r") as f:
                uuid = f.read()
            book = None
            for _book in books:  # get selected ebook
                if str(_book.uuid) == uuid:
                    book = _book
            # Check for renamed ebook
            if os.path.splitext(_file)[0] != book.name:
                original_book = book
                book = handle_renamed_book(book, shelf, _file)
                book.modified = True
                books[books.index(original_book)] = book
                print(
                    f"[cyan]Book [blue]{original_book.name}[/blue] has been renamed to [blue]{book.name}[/blue].")

            if not book:
                print(
                    f"[red]Could not find book [blue]{_file}[/blue]![/red]")
                continue
            if book.shelf == shelf:
                pass
                # print(
                #     f"[cyan]Did not any changes in [blue]{book.name}[/blue].")
            else:
                print(
                    f"[cyan]Shelf of book [blue]{book.name}[/blue] changed from [blue]{book.shelf}[/blue] to [blue]{shelf}[/blue].")
                book.shelf = shelf
                book.modified = True
                book.update()

    return books, shelfs


def update_db(cur, books, shelfs):
    # Update shelfs
    time = datetime.now().strftime("%Y-%m-%dT%H-%M-%SZ")
    print()
    for shelf in shelfs:
        if cur.execute("SELECT InternalName FROM Shelf WHERE InternalName = (?)", (shelf, )).fetchone():
            print(
                f"[cyan]Shelf [blue]{shelf}[/blue] already exists.")
            continue
        print(f"[cyan]Adding shelf [blue]{shelf}[/blue].")
        cur.execute("""INSERT INTO Shelf (
            CreationDate,
            Id,
            InternalName,
            LastModified,
            Name,
            _IsDeleted,
            _IsVisible,
            _IsSynced,
            LastAccessed
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);""", (time, shelf, shelf, time, shelf, False, True, False, time))

    # Update books
    print()
    for book in books:
        if not book.modified or book.shelf == DEFAULT_SHELF:
            continue
        if book.to_remove and cur.execute("SELECT ContentId FROM ShelfContent WHERE ContentId = (?)", (book.to_remove, )).fetchone():
            print(f"[cyan]Renaming book [blue]{book.name}[/blue]...")
            cur.execute("""UPDATE ShelfContent
            SET ContentId = (?)
            WHERE ContentId = (?);""", (book.db_path, book.to_remove))
        if not cur.execute("SELECT ContentId FROM ShelfContent WHERE ContentId = (?)", (book.db_path, )).fetchone():
            print(f"[cyan]Adding book [blue]{book.name}[/blue]...")
            cur.execute("""INSERT INTO ShelfContent (
                ShelfName,
                ContentId,
                DateModified,
                _IsDeleted,
                _IsSynced
            ) VALUES(?, ?, ?, ?, ?)""", (book.shelf, book.db_path, time, False, False))
        else:
            print(f"[cyan]Updating book [blue]{book.name}[/blue]...")
            cur.execute("""UPDATE ShelfContent
            SET ShelfName = (?)
            WHERE ContentId = (?);""", (book.shelf, book.db_path))


def empty_shelfes(cur):
    # Removing empty shelfs
    print()
    res = cur.execute("SELECT InternalName FROM Shelf")
    shelfs = res.fetchall()
    for shelf in shelfs:
        # Check if there are books stored in shelf
        if cur.execute("SELECT ContentId FROM ShelfContent WHERE ShelfName = (?)", (shelf[0], )).fetchone():
            # Books have been found, skip this shelf
            continue
        print(
            f"[cyan]Shelf [blue]{shelf[0]}[/blue] has been removed since it was empty.")
        cur.execute("DELETE FROM Shelf WHERE InternalName = (?)", (shelf[0], ))


rmtree(INTERACT_PATH)
os.mkdir(INTERACT_PATH)
db = sqlite3.connect(db_path)
cur = db.cursor()


books, shelfs = scan_books(cur)
place(books, shelfs)

print(
    f"[green]Successfully read books, you can now move them around in the '{INTERACT_PATH}' folder![/green]")
print("[yellow]Press 'enter' when you are done![/yellow]")
input("")

books, shelfs = update_with_changes(books)
update_db(cur, books, shelfs)
db.commit()
empty_shelfes(cur)
db.commit()
print("[green]Success![/green]")
# print(books)
