# GraphQL

Welcome to the GraphQL project, week 8 in CodeBoxx

The RESTapi is at https://rocketelevatorrestfulapi.azurewebsites.net/api/

## Testing

- If not already done, install Postman https://www.postman.com/downloads/
- Ask VincentMDfs for a link to the "Team Workspace"
- Once configured, you will have access to the dashboard
- Navigate to the left of your screen into the collections
- Enter the folder "CodeBoxx Week8 - Query Samples"
- Here you will find 2 folders, one for REST, one for GraphQL
- The calls included in this week's Documentation are already prepared
- Click on the request of your choice and a new tab will open
- Simply click the blue "Send" button to start the request

## To manually manipulate the requests

### GraphQL

- In your favorite browser, go to https://relevator.xyz
- Login as an admin (ask VincentMDfs is you need credentials)
- Navigate to the "BACKOFFICE" page under the "ADMIN" button in the navigation bar
- Depending on your query idea, verify the information in the table you wish to query
- Now in postman, navigate to the request you wish to modify
- The "query" details are to be modified to your taste before pressing "Send"
- See examples at the bottom of this readme
- Note that there is only one endpoint, so the url will remain unchanged

## See the RESTapi at:
  https://github.com/VincentMDfs/RESTapi2

## Examples of GraphQL modifications

{
  batteries(id: ?) {
    id
		status
  }
}

The question mark above can be replaced by any valid id number
"batteries" can also be replaced by "columns" or "elevators"
## For additionnal information, please contact our dev team
