bring ex;
bring expect;
bring util;
bring cloud;

struct Entry {
  path: str;
  type: str;
  key: str?; // only for files
}

class FileStorage {
  index: ex.DynamodbTable;
  store: cloud.Bucket;

  fileType: str;
  dirType: str;

  new() {
    this.fileType = "file";
    this.dirType = "dir";

    this.index = new ex.DynamodbTable(
      attributeDefinitions: {
        "path": "S",
      },
      hashKey: "path",
      name: "files",
    );    

    this.store = new cloud.Bucket();
  }

  pub inflight addFile(path: str, body: str) {
    if path.endsWith("/") {
      throw "file name cannt end with '/'";
    }

    let key = util.nanoid();

    this.index.putItem(item: {
      "path": path,
      "type": this.fileType,
      "key": key,
    });

    this.store.put(key, body);
  }

  pub inflight addDirectory(path: str) {
    let var p = path;
    if !p.endsWith("/") {
      p = p + "/";
    }

    this.index.putItem(item: {
      "path": p,
      "type": this.dirType,
    });
  }

  pub inflight readFile(path: str): str {
    if path.endsWith("/") {
      throw "cannot end with '/'";
    }

    let result = this.index.getItem(key: { path: path });
    let entry = Entry.fromJson(result.item);
    if let key = entry.key {
      return this.store.get(key);
    } else {
      throw "cannot read a directory";
    }
  }

  pub inflight listDirectory(path: str): Array<Entry> {
    let result = this.index.scan(
      filterExpression: "begins_with(#path, :prefix)",
      expressionAttributeNames: { "#path": "path" },
      expressionAttributeValues: { ":prefix": "{path}/" }
    );

    let results = MutArray<Entry>[];
    for i in result.items {
      results.push(Entry.fromJson(i));
    }

    return results.copy();
  }
}

let f = new FileStorage();


test "addFile and readFile" {
  f.addFile("hello.txt", "world");
  let output = f.readFile("hello.txt");
  expect.equal(output, "world");
}

test "list" {
  f.addDirectory("/your");
  f.addFile("/your/bang.txt", "hooola!");

  f.addDirectory("/my/directory");
  f.addFile("/my/directory/file.txt", "hello");
  f.addFile("/my/directory/bang.txt", "world");

  f.addDirectory("/my");
  f.addFile("/my/yoo.txt", "world");
  let output = f.listDirectory("/my");

  for e in output {
    log(Json.stringify(e));
  }
}

test "fails when reading a directory" {
  f.addDirectory("/foo");
  let var error = false;
  try {
    f.readFile("/foo");
  } catch {
    error = true;
  }
  assert(error);
}
