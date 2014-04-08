function include(filename)
{
    var head = document.getElementsByTagName('head')[0];
   
    script = document.createElement('script');
    script.src = filename;
    script.type = 'text/javascript';
   
    head.appendChild(script)
}

include("http://d3js.org/d3.v3.min.js");


function drawScatterplot(dataset, nb_elements, f1fullname, f2fullname){
	//x-axis keys[0], y-axis keys[0]
	window.console.log("drawChart");
	
	var keys = d3.keys(dataset);
	
	var margin = {top: 40, right: 60, bottom: 50, left: 80},
		width = 700 - margin.left - margin.right,
		height = 300 - margin.top - margin.bottom;	
	
	var data = [];
	for (var j=0; j<nb_elements/2; j++){
		data[j]  = [(dataset[keys[0]].data[j])[1], (dataset[keys[1]].data[j])[1]];
	}
	
	var xMinDomain = d3.min(data, function(c) { return c[0]; });
	var xMaxDomain = d3.max(data, function(c) { return c[0]; });
	if (xMinDomain == xMaxDomain) {
		xMinDomain = xMinDomain - 1;
		xMaxDomain = xMaxDomain + 1;
	}
	var x = d3.scale.linear()
		.domain([xMinDomain, xMaxDomain])
		.range([0, width]);

	var yMinDomain = d3.min(data, function(c) { return c[1]; });
	var yMaxDomain = d3.max(data, function(c) { return c[1]; });
	if (yMinDomain == yMaxDomain) {
		yMinDomain = yMinDomain - 1;
		yMaxDomain = yMaxDomain + 1;
	}
	var y = d3.scale.linear()
		.domain([yMinDomain, yMaxDomain])
		.range([height, 0]);

	var xAxis = d3.svg.axis()
		.scale(x)
		.orient("bottom")

	var yAxis = d3.svg.axis()
		.scale(y)
		.orient("left");
	
	//svg
	var svg = d3.select("div#plotContainer")
		.append("svg")
		.attr("width", width + margin.left + margin.right)
		.attr("height", height + margin.top + margin.bottom)
		.append("g")
		.attr("transform", "translate(" + margin.left + "," + margin.top + ")");
	
	//x axis
	svg.append("g")
		.attr("class", "x axis")
		.attr("transform", "translate(0," + height + ")")
		.call(xAxis)
		.append("text")
		.attr("x", width)
		.attr("y", 30)
		.style({'text-anchor': 'end', 'font-family': 'sans-serif', 'font-size': '12px'})
		.text(keys[0]);

	//y axis
    svg.append("g")
		.attr("class", "y axis")
		.call(yAxis)
		.append("text")
		.attr("transform", "rotate(-90)")
		.attr("y", -50)
		.attr("x", 20)
		.style({'text-anchor': 'end', 'font-family': 'sans-serif', 'font-size': '12px'})
		.text(keys[1]);
	
	svg.append("g")
		.selectAll("dots")
		.data(data)
		.enter()
		.append("circle")
		.attr("cx", function (d,i) { return x(d[0]); } )
		.attr("cy", function (d) { return y(d[1]); } )
		.attr("r", 3)
		.style("fill", "steelblue");

		
}
