var rootUrl = "../service.html";
var xmlData = { 'a': 'a' }; 

function encode(xmlText) {
    xmlText = xmlText.replace(/</g, "&lt;");
    xmlText = xmlText.replace(/>/g, "&gt;");
    xmlText = xmlText.replace(/\"/g, "&quot;");
    return xmlText;
}

function encodePlusIndent(xmlText) {
    xmlText = xmlText.replace(/</g, "&lt;");
    xmlText = xmlText.replace(/>/g, "&gt;");
    xmlText = xmlText.replace(/\"/g, "&quot;");
    return xmlText;
}

function callAjax(xmlData, reqDiv, sucessF){
    reqDiv.innerHTML = "<h2>Processing...</h2>";
    $.ajax({
	    type: "POST",
	    url: rootUrl,
	    data: xmlData,
	    cache:false,
	    dataType: "text",
	    success: sucessF
    });        
}

function showSucess(header, resData, resDiv) {
    var isError = (resData.indexOf("<error>") != -1) ;
    var fontStyle = "<font color=\"black\">";	
    if ( isError) fontStyle = "<font color=\"red\">";

    var headerText = "<h2>" + header + " Response Details</h2>";	
    if ( isError) headerText = "<h2>" + header + " Processing Error</h2>";	

    resDiv.innerHTML = "<div><code>" + fontStyle + 
	    headerText + encodePlusIndent(resData) + "</font></code></div>";
}
