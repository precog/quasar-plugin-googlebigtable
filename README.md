# quasar-plugin-googlebigtble [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/pSSqJrr)

## Usage

```sbt
libraryDependencies += "com.precog" %% "quasar-plugin-googlebigtable" % <version>
```

## Configuration

The configuration of the Google BigTable datasource has the following JSON format

```
{
  "auth": JSON,
  "instance": String,
  "table": String,
  "rowPrefix": String
}
```

* `auth` - the Service Account key that has permission to read the configured table.
* `instance` - the name of the instance the table resides in.
* `table` - the name of the table (called 'table ID' in the console of Google Cloud Platform).
* `rowPrefix` - the prefix to filter on. The empty string means no filtering.