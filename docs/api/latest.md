# API Docs - v1.0.0-SNAPSHOT

## Sink

### live *<a target="_blank" href="http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#sink">(Sink)</a>*

<p style="word-wrap: break-word"> </p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@sink(type="live", @map(...)))
```

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
 
```
<p style="word-wrap: break-word"> </p>

## Source

### live *<a target="_blank" href="http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#source">(Source)</a>*

<p style="word-wrap: break-word"> </p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@source(type="live", sql.query="<STRING>", host.name="<STRING>", api.key="<STRING>", table.name="<STRING>", database.name="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">sql.query</td>
        <td style="vertical-align: top; word-wrap: break-word">The SQL select query</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">host.name</td>
        <td style="vertical-align: top; word-wrap: break-word">The Hostname</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">api.key</td>
        <td style="vertical-align: top; word-wrap: break-word">The api Key</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">table.name</td>
        <td style="vertical-align: top; word-wrap: break-word">Database table name</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database.name</td>
        <td style="vertical-align: top; word-wrap: break-word">Database name</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@source(type = 'live', sql.query='Select * from table', 
host.name='api-varden-example',
api.key = 'apikey-xxxxxxxxx', database.name = 'databaseName', table.name = 'TableName', 
@map(type='keyvalue'), @attributes(id = 'id', name = 'name'))
define stream inputStream (id int, name string)
```
<p style="word-wrap: break-word">In this example, the Live source executes the select query. Theevents consumed by the source are sent to the inputStream</p>

