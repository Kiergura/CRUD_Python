My first CRUD Python project.
Connects to a database using sqlite 3 to select, insert, update and delete information.
It also includes functions to filter through information, make sure it is to some degree of the correct type and adjust were it would be reasonable to.
Used Postman to test the functions, the script would receive json files which it would verify and execute accordingly.
Json examples on the "Code" section of this document.

<!-- View
Example json
{
  "filter":[
    {
      "quantity": ["", "operator"],
      "name": ["", "operator"],
      "id": 0
      },
    {
      "name": ["", "", "operator('--' in the case of two values)"]
    }
  ]
} -->

<!-- Add products
Example json
{
    "user": "admin",
    "data": [
        {"name": "", "quantity": 0},
        {"name": "", "quantity": 0},
    ]
} -->

<!-- Remove products
Example json
{
    "user": "admin",
    "data": [
        {"name": ""}
    ]
} -->

<!-- Edit products
Example json
{
    "user": "admin",
    "data": [
        {"name": "", "new quantity": 0, "new name": ""},
        {"name": "", "new name": ""}
    ]
} -->

<!-- Add clients
Example json
{
    "user": "admin",
    "data":[
        {"name": ""},
        {"name": ""},
        {"name": ""}
    ]
} -->

<!-- Remove clients
Example json
{
    "user": "",
    "data":[
        {
            "name": ""
        }
    ]
} -->

<!-- Edit clients
Example json
{
    "user": "",
    "data":[
        {"new name": ""}
    ]
} -->
