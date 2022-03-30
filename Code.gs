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

  fields.newMetric()
    .setId('billsec')
    .setName('Duration ')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);

  fields.newDimension()
    .setId('calldate')
    .setName('Call Date and Time')
    .setType(types.YEAR_MONTH_DAY_SECOND);

  fields.newMetric()
    .setId('charge')
    .setName('Charge')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);

  fields.newDimension()
    .setId('clid')
    .setName('Caller ID')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('destination')
    .setName('Direction')
    .setType(types.TEXT);  
  
  fields.newDimension()
    .setId('disposition')
    .setName('Disposition')
    .setType(types.TEXT);

  fields.newDimension()
    .setId('dst')
    .setName('Destination')
    .setType(types.TEXT);
  
  fields.newMetric()
    .setId('duration')
    .setName('Duration - total')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newDimension()
    .setId('id')
    .setName('CDR ID')
    .setType(types.TEXT);  
 
  fields.newMetric()
    .setId('rate')
    .setName('Per min rate')
    .setType(types.NUMBER)
    .setAggregation(aggregations.AVG);  

  fields.newDimension()
    .setId('src')
    .setName('Source')
    .setType(types.TEXT);
  
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

function responseToRows(requestedFields, data) {
  // Transform parsed data and filter for requested fields
  var count = 0;
  var fields = requestedFields.asArray();
  return data.map(function(cdr) {
    var row = [];
    fields.forEach(function (field) {
      switch (field.getId()) {
        case 'billsec':
          return row.push(cdr.billsec);
        case 'calldate':
          return row.push(cdr.calldate.replace(/[-:\s]/g, ''));
        case 'charge':
          return row.push(cdr.charge);
        case 'clid':
          return row.push(cdr.clid);
        case 'destination': // Direction
          if (cdr.type == 'O') {
            return row.push('Outbound');
          } else if (cdr.type == 'I') {
            return row.push('Inbound');
          } else if (cdr.type == 'M') {
            return row.push('Missed');
          } else {
            return row.push('Unknown ' + cdr.type);
          }
        case 'disposition':
          return row.push(cdr.disposition);
        case 'dst':
          return row.push(cdr.dst);
        case 'duration':
          return row.push(cdr.duration);
        case 'id':
          return row.push(cdr.id);
        case 'rate':
          return row.push(cdr.rate);
        case 'src':
          return row.push(cdr.src);
        default:
          return row.push('');
      }
    });

    count++;
    if (count <= 10) {
      console.log(row);
    }

    return { values: row };
  });
}

function getData(request) {
 
  console.log('getData, request:');
  console.log(request);
  // {dateRange={endDate=2022-03-28, startDate=2022-03-01}, scriptParams={lastRefresh=1648522449802}, fields=[{name=billsec}, {name=calldate}]}

  var requestedFieldIds = request.fields.map(function(field) {
    return field.name;
  });
  var requestedFields = getFields().forIds(requestedFieldIds);

  var userProperties = PropertiesService.getUserProperties();
  var username = userProperties.getProperty('voipstudio.username');
  var token = userProperties.getProperty('voipstudio.token');

  var hash = Utilities.base64Encode(username + ":" + token);

  var dateRange = request.dateRange;

  var filter = [
    {
      operator: 'gt',
      property: 'calldate',
      value: dateRange.startDate + ' 00:00:00'
    },{
      operator: 'lt',
      property: 'calldate',
      value: dateRange.endDate + ' 23:59:59'
    }
  ];
  
  var options = {
    'method' : 'GET',
    'headers': {
      'Authorization': 'Basic ' + hash,
      'Content-Type': 'application/json'
    },
    'muteHttpExceptions':true
  };

  var data = [];
  var page = 1;
  var limit = 5000;
  var maxPage = 500000 / limit;
  var total = null;
  var startTime = new Date().getMilliseconds();
  

  while (page <= maxPage) {

    var url = 'https://l7api.com/v1.2/voipstudio/cdrs?page='+page+'&limit='+limit+'&filter=';

    console.log('getData GET ' + url + JSON.stringify(filter));

    var response = UrlFetchApp.fetch(url + encodeURIComponent(JSON.stringify(filter)), options);
    if (response.getResponseCode() == 200) {
      var parsedResponse = JSON.parse(response);

      if (total === null) {
        total = parsedResponse.total;
      }

      if (!total) {
        break;
      }

      parsedResponse.data.forEach(function(record) {
        data.push(record);
      });

      if (data.length == total) {
        break;
      }

    } else {
      DataStudioApp.createCommunityConnector()
      .newUserError()
      .setDebugText('Error fetching data from API. Exception details: ' + response)
      .setText('Error fetching data from API. Exception details: ' + response)
      .throwException();
    }

    page++;
  }

  if (data.length !== total) {
    DataStudioApp.createCommunityConnector()
    .newUserError()
    .setDebugText('Error fetching data from API. Expecte to get total ['+total+'] found instead ['+data.length+']')
    .setText('Error fetching data from API. Expecte to get total ['+total+'] found instead ['+data.length+']')
    .throwException();
  }
  
  var timeDiff = new Date().getMilliseconds() - startTime;
  
  console.log('getData fetched ['+data.length+'] records from API in '+timeDiff+' ms.');

  return {
    schema: requestedFields.build(),
    rows: responseToRows(requestedFields, data),
    filtersApplied: false
  };
}
