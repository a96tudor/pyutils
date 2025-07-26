class Attachment:
    def __init__(self, filename: str, content: bytes):
        self.filename = filename
        self.content = content

    def __repr__(self):
        return f"Attachment(filename={self.filename})"

    def __str__(self):
        return f"Attachment: {self.filename}"
