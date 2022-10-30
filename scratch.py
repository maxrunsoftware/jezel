from abc import ABC, abstractmethod


class Database(ABC):
    def open_connection(self) -> str:
        print("open_connection()")
        return "open_connection()"

    @property
    @abstractmethod
    def metadata(self) -> str: raise NotImplementedError()


class InsertMixin(ABC):
    @property
    @abstractmethod
    def open_connection(self): raise NotImplementedError()

    def insert(self, a: str):
        print(f"insert(a={a})")
        v = self.open_connection()
        print(f"  {v}")


class DeleteMixin(ABC):
    @property
    @abstractmethod
    def open_connection(self): raise NotImplementedError()

    def delete(self, b: str):
        print(f"insert(a={b})")
        v = self.open_connection()
        print(f"  {v}")


class MainClass(Database, InsertMixin, DeleteMixin):
    @property
    def metadata(self) -> str:
        return "metadata"

    def work(self):
        self.open_connection()
        self.insert("varA")
        self.delete("varB")

