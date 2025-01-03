from flask import Flask, jsonify, send_file, request

app = Flask(__name__)

# Browser:
#
# > http://127.0.0.1:5000/jstowns
#
# DuckDB use `read_json`:
#
# > SELECT * FROM read_json('http://127.0.0.1:5000/jstowns');
#
# Excel / PowerQuery - apropos this:
# 
# > let
# >     Source = Json.Document(Web.Contents("http://127.0.0.1:5000/jstowns")),
# >     #"Converted to Table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
# >     #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to Table", "Column1", {"name", "line"}, {"Name", "Line"})
# > in
# >     #"Expanded Column1"
#
@app.route("/jstowns", methods=["GET"])
def get_jstowns():
    return jsonify(
        [
            {"name": "Bingley", "line": "Airedale"},
            {"name": "Ilkley", "line": "Wharfedale"},
            {"name": "Keighley", "line": "Airedale"},
            {"name": "Shipley", "line": "Airedale"},
        ]
    )

# Browser (asks to download):
#
# > http://127.0.0.1:5000/towns.csv
#
# For DuckDB end point must be a file name unless you use `read_csv` function:
#
# > SELECT * FROM 'http://127.0.0.1:5000/towns.csv';
#
# Better to use use `read_csv`:
#
# > SELECT * FROM read_csv('http://127.0.0.1:5000/towns.csv');
#
@app.route("/towns.csv", methods=["GET"])
def get_towns_csv():
    return send_file("..\\data\\towns.csv")

# Browser (asks to download):
#
# > http://127.0.0.1:5000/csv
#
# DuckDB use `read_csv`
#
# > SELECT * FROM read_csv('http://127.0.0.1:5000/csv');
#
# Excel / PowerQuery - apropos this:
# 
# > let
# >     Source = Csv.Document(Web.Contents("http://127.0.0.1:5000/csv"),[Delimiter=","]),
# >     #"Changed Type" = Table.TransformColumnTypes(Source,{{"Column1", type text}, {"Column2", type text}}),
# >     #"Promoted Headers" = Table.PromoteHeaders(#"Changed Type", [PromoteAllScalars=true]),
# >     #"Changed Type1" = Table.TransformColumnTypes(#"Promoted Headers",{{"Town", type text}, {"Line", type text}})
# > in
# >     #"Changed Type1"
#
@app.route("/csv", methods=["GET"])
def get_csv():
    return send_file("..\\data\\towns.csv")


# Browser:
#
# > http://127.0.0.1:5000/arg_test/?sort=asc&limit=5
#
# "Get Args"
#
@app.route('/arg_test/')
def arg_test():
    sort =  request.args.get("sort")
    limit = request.args.get("limit")
    return f"sort order = {sort}, limit = {limit}"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
