# log-parser
Small and simple Java service for processing logs

The service reads from the directory log files with a certain format (example.log):  
IP, username, date, event + (event number), status  

Service can get structured requests from console and process them.  
Structure:
- "get (parameter)";
- "get (parameter) for (prameter)";
- "get (parameter) for (parameter) and date between (date) and (date) " or "get (parameter) date between (date) and (date)".  

Returns results to the console.
