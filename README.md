edge-sql-plus
=============

This is an enhancement to the [edge-sql](http://tjanczuk.github.com/edge-sql) project by Tomasz Janczuk.  In `edge-sql-plus` I have added the ability to execute stored procedures and pass TVP or Table-Valued Parameters. In addition, it allows for muliple result sets (not MARS but rather multiple record sets).

**Full Example**
```sh
var execProc = edge.func('sql-plus', {
    source: 'exec myStoredProcedure',
    connectionString: 'SERVER=myserver;DATABASE=mydatabase;Integrated Security=SSPI'
});

execProc(
    {
        normalParameter: 100,
        myTvp :
        {
            UdtType: 'MyTableValuedParameterDataType',
            Rows: [
                { column1: 'value1', someothercolumn: 2 }
            ]
        }
    },
    function(err, result) {
        if (err) {
            ... do your error handling ...
        }
        ... process the result ...
    });
        
```

The `execProc` portion is almost identical to the original *edge-sql* way.  In *edge-sql-plus* we add the ability to pass in a JSON object as a parameter.  This JSON object, **myTvp** in the example above, needs to have two properties defined:
- `UdtType` - This is a string value that corresponds to the user defined type of your table valued parameter.
- `Rows` - The data to be used in the TVP.  This should be an array of JSON objects with the exact same number of properties as the TVP is expecting.

The edge-sql-plus compiler (a slightly modified version of Tomasz's edge-sql compiler) detects the TVP property and converts the Rows property into an array of SqlDataRecord objects.

**Multiple Result Sets**
Stored procedures in SQL Server can return more than one result set.  `edge-sql-plus` will detect multiple result sets and output an array of result sets in this situation.  If there is only one result set detected the results will only contain the array of rows from the single result set.

Consider the following:
```sh
SELECT * FROM Table1;
```
The statement above will result in a result with an array of rows.
```sh
[
    { column1: 1, column2: 2 ... },
    { column1: 1, column2: 2 ... }
]
```
However this query will output multiple result sets:
```sh
SELECT * FROM Table1; SELECT * FROM Table2;
```
The statement above will result in a result with an array of rows.
```sh
[
    [
        { column1: 1, column2: 2 ... },
        { column1: 1, column2: 2 ... }
    ],
    [
        { column1: 1, column2: 2 ... },
        { column1: 1, column2: 2 ... }
    ]
]
```

---


##### Original Readme 

This is a SQL compiler for edge.js. It allows accessing SQL databases from Node.js using Edge.js and ADO.NET. 

See [edge.js overview](http://tjanczuk.github.com/edge) and [edge.js on GitHub](https://github.com/tjanczuk/egde) for more information. 
