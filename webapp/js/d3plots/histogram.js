function include(filename)
{
    var head = document.getElementsByTagName('head')[0];
   
    script = document.createElement('script');
    script.src = filename;
    script.type = 'text/javascript';
   
    head.appendChild(script)
}

include("http://d3js.org/d3.v3.min.js");

function drawHistogram(dataset, nb_elements){
	drawChartHistogram(parseDataHistogram(dataset, nb_elements), nb_elements,  d3.keys(dataset));
}

function parseDataHistogram(dataset, nb_elements){
	var keys = d3.keys(dataset);
	var data_array = [];
	for (var j=0; j<nb_elements; j++){
		data_array[j]  = (dataset[keys[0]].data[j])[1];
	}
	var histogram = d3.layout.histogram();
	return histogram(data_array);
}

function drawChartHistogram(data, nb_elements, f1fullname){
	
	var margin = {top: 40, right: 60, bottom: 50, left: 80},
		width = 700 - margin.left - margin.right,
		height = 300 - margin.top - margin.bottom;	
	
	var x = d3.scale.ordinal()
		.domain(data.map(function(d) { return d.x+d.dx/2; }))
		.rangeRoundBands([0, width], .1);

	var yMaxDomain = d3.max(data, function(c) { return c.y; });
	if (0 == yMaxDomain) {
		yMaxDomain = 1;
	}
	var y = d3.scale.linear()
		.domain([0, yMaxDomain])
		.range([height, 0]);

	var xAxis = d3.svg.axis()
		.scale(x)
		.orient("bottom")
		.tickFormat(d3.format(".2f"));

	var yAxis = d3.svg.axis()
		.scale(y)
		.orient("left")
		.ticks(yMaxDomain > 10 ? 10 : yMaxDomain);
	
	//svg
	var svg = d3.select("div#plotContainer")
		.append("svg")
		.attr("width", width + margin.left + margin.right)
		.attr("height", height + margin.top + margin.bottom)
		.append("g")
		.attr("transform", "translate(" + margin.left + "," + margin.top + ")");
		
	var synchronizedMouseOver = function() {
		var bar = d3.select(this);
		var indexValue = bar.attr("index_value");

		var barSelector = "rect.-bar-" + indexValue;
		var selectedBar = d3.selectAll(barSelector);
		selectedBar.style("fill", "black");
	};

	var synchronizedMouseOut = function() {
		var bar = d3.select(this);
		var indexValue = bar.attr("index_value");

		var barSelector = "rect.-bar-" + indexValue;
		var selectedBar = d3.selectAll(barSelector);
		selectedBar.style("fill", "steelblue");
	};
	  
	var synchronizedMouseClick = function(d,i) {
		$('#plotContainer').html('');
		var histogram = d3.layout.histogram();
		drawChartHistogram(histogram(d), d.y,  f1fullname);
      };
	

	svg.append("g")
		.attr("class", "x axis")
		.attr("transform", "translate(0," + height + ")")
		.call(xAxis)
		.append("text")
		.attr("x", width)
		.attr("y", 30)
		.style("text-anchor", "end")
		.text("Values");

	//y axis
    svg.append("g")
		.attr("class", "y axis")
		.call(yAxis)
		.append("text")
		.attr("transform", "rotate(-90)")
		.attr("y", -50)
		.attr("x", 26)
		.style("text-anchor", "end")
		.text("Frequency");
	
	svg.append("g")
		.selectAll(".bars")
		.data(data)
		.enter()
		.append("rect")
        .attr("width", x.rangeBand())
		.attr("x", function(d) { return x(d.x+d.dx/2); })
		.attr("y", function(d) { return y(d.y); })
		.attr("height", function(d) { return y.range()[0] - y(d.y); })
		.attr("fill","steelblue")
		.on('mouseover', synchronizedMouseOver)
        .on("mouseout", synchronizedMouseOut)
		.on("click", synchronizedMouseClick)
		.attr("class", function(d, i) { return "-bar-index-" + i; })
        .attr("index_value", function(d, i) { return "index-" + i; })
		.order();
}