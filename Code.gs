function isAdminUser() {
  return true;
}

function isAuthValid() {
  var userProperties = PropertiesService.getUserProperties();
  var username = userProperties.getProperty('voipstudio.username');
  var token = userProperties.getProperty('voipstudio.token');
  return checkForValidKey({username: username, token: token});
}

function getConfig(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();

  config
    .newInfo()
    .setId('usdInfo')
    .setText('You can use this connector to import VoIPstudio CDRs into Google Data Studio');

  config.setDateRangeRequired(true);
  config.setIsSteppedConfig(false);

  return config.build();
}

function getAuthType() {
  var cc = DataStudioApp.createCommunityConnector();
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.USER_TOKEN)
    .setHelpUrl('https://voipstudio.com/docs/administrator/introduction/')
    .build();
}

function getFields(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;
  
  fields.newDimension()
    .setId('disposition')
    .setType(types.TEXT);
  
  fields.newMetric()
    .setId('duration')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newDimension()
    .setId('calldate')
    .setType(types.YEAR_MONTH_DAY_SECOND);
  
  return fields;
}

function getSchema(request) {
  var fields = getFields(request).build();
  return { schema: fields };
}

function checkForValidKey(credentials) {
  console.log('checkForValidKey');
  var token = credentials.username + ":" + credentials.token;
  var hash = Utilities.base64Encode(token); 
  
  var baseURL = 'https://l7api.com/v1.2/voipstudio/me';
  var options = {
    'method' : 'GET',
    'headers': {
      'Authorization': 'Basic ' + hash,
      'Content-Type': 'application/json'
    },
    'muteHttpExceptions':true
  };
  var response = UrlFetchApp.fetch(baseURL, options);
    
  if (response.getResponseCode() == 200) {
    return true;
  } else {
    return false;
  }
}

function setCredentials(request) {
  var validKey = checkForValidKey(request.userToken);
  if (!validKey) {
    return {
      errorCode: 'INVALID_CREDENTIALS'
    };
  }
  var userProperties = PropertiesService.getUserProperties();
  userProperties.setProperty('voipstudio.username', request.userToken.username);
  userProperties.setProperty('voipstudio.token', request.userToken.token);
  return {
    errorCode: 'NONE'
  };
}

function resetAuth() {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty('voipstudio.username');
  userProperties.deleteProperty('voipstudio.token');
}

function responseToRows(requestedFields, response) {
  // Transform parsed data and filter for requested fields
  return response.data.map(function(cdr) {
    var row = [];
    requestedFields.asArray().forEach(function (field) {
      switch (field.getId()) {
        case 'calldate':
          return row.push(cdr.calldate.replace(/-/g, '').replace(/:/g,''));
        case 'duration':
          return row.push(cdr.duration);
        case 'disposition':
          return row.push(cdr.disposition);
        default:
          return row.push('');
      }
    });
    return { values: row };
  });
}

function getData(request) {
 
  console.log('getData');
  console.log(request);
  // {dateRange={endDate=2022-03-28, startDate=2022-03-01}, scriptParams={lastRefresh=1648522449802}, fields=[{name=billsec}, {name=calldate}]}

  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  var requestedFields = getFields().forIds(requestedFieldIds);

  console.log('requestedFields:');
  console.log(requestedFields);
  var userProperties = PropertiesService.getUserProperties();
  var username = userProperties.getProperty('voipstudio.username');
  var token = userProperties.getProperty('voipstudio.token');

  var hash = Utilities.base64Encode(username + ":" + token);
  
  // filter: [{"operator":"gt","value":"2022-02-28 23:00:00","property":"calldate"},{"operator":"lt","value":"2022-03-28 22:00:00","property":"calldate"}]
  console.log('request:');
  console.log(request);
  
  var baseURL = 'https://l7api.com/v1.2/voipstudio/cdrs?limit=20';
  var options = {
    'method' : 'GET',
    'headers': {
      'Authorization': 'Basic ' + hash,
      'Content-Type': 'application/json'
    },
    'muteHttpExceptions':true
  };
  var response = UrlFetchApp.fetch(baseURL, options);
  if (response.getResponseCode() == 200) {
    var parsedResponse = JSON.parse(response);
    var output = {
      schema: requestedFields.build(),
      rows: responseToRows(requestedFields, parsedResponse),
      filtersApplied: false
    };
    console.log('output:');
    console.log(output);

    return output;
  } else {
    DataStudioApp.createCommunityConnector()
    .newUserError()
    .setDebugText('Error fetching data from API. Exception details: ' + response)
    .setText('Error fetching data from API. Exception details: ' + response)
    .throwException();
  }
}
