var rootUrl = "../service.html";
var xmlData = {}; 

//var guestKey = "5D74C6768EFBEC9C2CCD52607F47E6DD";
var guestKey = "main";

function encode(xmlText) {
    xmlText = xmlText.replace(/</g, "&lt;");
    xmlText = xmlText.replace(/>/g, "&gt;");
    xmlText = xmlText.replace(/\"/g, "&quot;");
    return xmlText;
}

function decode(xmlText) {
    return xmlText;
}

function getUrlVars() {     
    var vars = [], hash;
    var hashes = window.location.href.slice(window.location.href.indexOf('?') + 1).split('&');
    for(var i = 0; i < hashes.length; i++) {
        hash = hashes[i].split('=');
        vars.push(hash[0]);
        vars[hash[0]] = hash[1];
    }
    return vars;
}


function callAjax(xmlData, resDiv, sucessF){
    resDiv.innerHTML = "<h2>Processing...</h2>";
	$.ajax({
	    type: "POST",
	    url: rootUrl,
	    data: xmlData,
	    cache:false,
	    dataType: "text",
	    success: sucessF
    });        
}

function showSucess(resData, resDiv) {
            		
    var isError = (resData.indexOf("<error>") != -1) ;
    if ( isError) {
        fontStyle = "<font color=\"red\">";
        headerText = "<h3>Processing Error</h3>";	
        resDiv.innerHTML = "<pre><code>" + 
            fontStyle + headerText + "</font></pre>";
    }
    return isError;
}

function getScrollTop(){     
    if( typeof pageYOffset!= 'undefined'){         
        //most browsers         
        return pageYOffset;     
    }     else {         
        var B= document.body; //IE 'quirks'         
        var D= document.documentElement; 
        //IE with doctype         
        D= (D.clientHeight)? D: B;         
        return D.scrollTop;     
    }
} 