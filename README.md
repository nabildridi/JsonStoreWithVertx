# A Json Store with Vertx

A basic Json documents store built with Vertx, you can perform all kind of CRUD operations on the Json documents with http requests.


# Operations list

With http requests, you can :

 - Insert a Json document
 - Update the whole document
 - Update a **part** of the document (partial update)
 - Delete a document
 - Request a Json document by id
 - Request one or multiple **parts** of a document by id
 - The usual query operations :
	 - Page number
	 - page size
	 - Sort field
	 - Sort order
	 - Filter
	 - Send only one or multiple **parts** of the documents 

## Limitations

This a very basic json store, it doesn't have the concept of collections or other advanced functionalities.
It's just a personal project playing with Vertx.

## Build

build the fatjar with :

    mvn clean install

and just execute the jar with :

    java -jar


## Configuration

### Application configuration :
The application configuration is **conf/config.json**


```json
 {
	"application_port" : 8080,
	"store_fs_path" : "d:\\.jsonStore",
	"cache_size" : 100000,
	"cache_preload" : true
}
```

 - **application_port** : the http port of the application
 - **store_fs_path** : the location of folder where the json files wil be stored, if omitted then the files will be saved in **<user.home>/.jsonStore**
 - **cache_size** : the maximum size of the cache
 - **cache_preload** : if true the application will preload the json documents into the cache at the startup until the maximun size is reached or there is no more documents to cache, if activated the apllication startup wil take some time (took 40 seconds to preload 33000 json files from an ssd during tests) but will gain maximum performance later.

### Http basic authentication configuration :
Every request must provide http basic authentication (username and password), the usernames and passwords can be configured in **vertx-users.properties** file.

The default username and password are **admin** and **password**


# Operations

## Insert a Json document

 - Path : **/**
 - Http method : **POST**
 - Header : **Content-Type: application/json**
 - **Basic authentication must be present**
 - Request body : the json to insert 

The response will be the provided document with the Id added.

**Generated id vs provided id** : 
the user can provide a custom id to the document by adding the attribute **_systemId** the Json root of the document before sendinf the request, if omitted then the application will it an auto generated id.

Example:
**Request body :**
```json
{
	"glossary": {
		"title": "example glossary"
	}
}
```
**Response body :**
```json
{
	"glossary": {
		"title": "example glossary"
	},
	"_systemId": "9ECJgNaQWeQIRLy0"
}
```

## Update the whole document

 - Path : **/**
 - Http method : **POST**
 - Header : **Content-Type: application/json**
 - **Basic authentication must be present**
 - Request body : the json to update

The Json sent to the application must contain the original id.

Example:
**Request body :**
```json
{
	"glossary": {
		"title": "another example"
	},
	"_systemId": "9ECJgNaQWeQIRLy0"
}
```
**Response body :**
```json
{
	"glossary": {
		"title": "another example"
	},
	"_systemId": "9ECJgNaQWeQIRLy0"
}
```
## Get a document by id

 - Path : **/{id}?extract=????**
 - Http method : **GET**
 - **Basic authentication must be present**

Request a document by id and optionally with a comma separated list of attributes you want to get instead of the whole document.

***Example:***
**Request :**

http://localhost:8080/9ECJgNaQWeQIRLy0

**Response body :**
```json
{
	"glossary": {
		"title": "another example"
	},
	"_systemId": "9ECJgNaQWeQIRLy0"
}
```

***Example 2:***
**Whole document :**
```json
{
	"glossary": {
		"title": "example glossary",
		"GlossDiv": {
			"title": "S",
			"GlossList": {
				"GlossEntry": {
					"ID": "SGML",
					"SortAs": "SGML",
					"GlossTerm": "Standard Generalized Markup Language",
					"Acronym": "SGML",
					"Abbrev": "ISO 8879:1986",
					"GlossDef": {
						"para": "A meta-markup language, used to create markup languages such as DocBook.",
						"GlossSeeAlso": [
							"GML",
							"XML"
						]
					},
					"GlossSee": "markup"
				}
			}
		}
	},
	"_systemId": "9EE8TAxGnYL7PvbK"
}
```

**Request :**

http://localhost:8080/9ECJgNaQWeQIRLy0?extract=glossary.title,glossary.GlossDiv.GlossList.GlossEntry.GlossTerm

**Response body :**
```json
{
	"glossary": {
		"title": "example glossary",
		"GlossDiv": {
			"GlossList": {
				"GlossEntry": {
					"GlossTerm": "Standard Generalized Markup Language"
				}
			}
		}
	},
	"_systemId": "9EE8TAxGnYL7PvbK"
}
```



## Partial update

 - Path : **/**
 - Http method : **PUT**
 - Header : **Content-Type: application/json**
 - **Basic authentication must be present**
 - Request body : the fragments that will be inserted and replaced to the main document

The response will be the updated main document.

Example:
**Request body :**
```json
{
	"glossary": {
		"isbn": "978-1-56619-909-4"
	},
	"GlossDiv": {
		"title": "S"
	},
	"_systemId": "9ECJgNaQWeQIRLy0"
}
```
**Response body :**
```json
{
	"glossary": {
		"isbn": "978-1-56619-909-4",
		"title": "another example"
	},
	"GlossDiv": {
		"title": "S"
	},
	"_systemId": "9ECJgNaQWeQIRLy0"
}
```


## Delete

### Delete by Id

 - Path : **/{id}**
 - Http method : **DELETE**
 - Header : **Content-Type: application/json**
 - **Basic authentication must be present**
 - Request body : empty
 
The response will be a json containing the **_systemId** of the deleted document.

### Delete by request body

 - Path : **/**
 - Http method : **DELETE**
 - Header : **Content-Type: application/json**
 - Request body : Json containing the **_systemId** of the document to delete

The response will be a json containing the **_systemId** of the deleted document.


# Pagination, sort, filtering and fragments

 - Path : **/query**
 - Http method : **POST**
 - Header : **Content-Type: application/json**
 - **Basic authentication must be present**
 - Request body : Json containing the requested pagination, sort, filtering and fragments

An example of a full request :

```json
{
	"page": 0,
	"size": 10,	
	"sortField": "glossary.title",
	"sortOrder": 1,
	"filter": "$.glossary.GlossDiv[?(@.title =='S' && @.GlossList.GlossEntry.ID=='SGML')]",
	"extract": "glossary.title , glossary.GlossDiv.GlossList.GlossEntry.ID"
}
```

## Pagination

```json
{
	"page": 0,
	"size": 10	
}
```

 - **page :** page number, starts with 0 and can't be negative
 - **size :**  page size

## Sorting


```json
{
	"sortField": "glossary.title",
	"sortOrder": 1
}
```

 - **sortField** : path of the field
 - **sortOrder :** 1 for ascending order, -1 for descending

## Filters

```json
{
	"filter": "$.glossary.GlossDiv[?(@.title =='S' && @.GlossList.GlossEntry.ID=='SGML')]"
}
```

 - **Filter :** a  **JsonPath** query  [(github.com/json-path/JsonPath)](https://github.com/json-path/JsonPath)

## Fragments to extract


```json
{
	"extract": "glossary.title , glossary.GlossDiv.GlossList.GlossEntry.ID"
}
```

 - **extract** : a comma separated list of the attibutes to be returned in the response, if absent then the whole document will be sent


