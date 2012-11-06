	    var rootUrl = "../service.html";
	    var xmlData = {}; 

	    var guestKey = "75A7659CCA1FCB4C485A83D3BC022AFB";

        function encode(xmlText) {
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
        
		function showSucess(header, reqDiv, resData, resDiv) {
            var requestXml = "<pre><code><h3>" + header + 
                " Request Details</h3><strong>URL: </strong>" + rootUrl + 
	                "<br /><strong>POST Parameters</strong>" ;
	        $.each(xmlData, 
	            function(key, value) { 
                    requestXml = requestXml + "<br />" + key + "=" + encode(value);
                }
            );
	                 
            requestXml = requestXml + "</pre></code>";
	                		
		    reqDiv.innerHTML = "<h2>" + header + "</h2>" + requestXml;

		    var isError = (resData.indexOf("<error>") != -1) ;
		    var fontStyle = "<font color=\"black\">";	
		    if ( isError) fontStyle = "<font color=\"red\">";

		    var headerText = "<h3>" + header + " Response Details</h3>";	
		    if ( isError) headerText = "<h3>" + header + " Processing Error</h3>";	

		    resDiv.innerHTML = "<pre><code>" + fontStyle + 
			    headerText + encode(resData) + "</font></pre>";
		}	    