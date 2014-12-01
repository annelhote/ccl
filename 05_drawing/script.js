$(document).ready(
    function() {
        // Common settings
        var margin = {
                top: 20,
                right: 20,
                bottom: 30,
                left: 50
            },
            width = 960 - margin.left - margin.right,
            height = 500 - margin.top - margin.bottom;
        
        var x = d3.scale.linear()
            .range([0, width]);

        var y = d3.scale.linear()
            .range([height, 0]);
        
        // First graph : Volume of emails exchanged
        var xAxis1 = d3.svg.axis()
            .scale(x)
            .orient("bottom");

        var yAxis1 = d3.svg.axis()
            .scale(y)
            .orient("left");

        
        var line1 = d3.svg.line()
            .x(function(d) {
                return x(d.year);
            })
            .y(function(d) {
                return y(d.mails);
            });

        var svg1 = d3.select("#volumeOfEmailsExchanged").append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        d3.json("data.json", function(error, data) {
            // Remove values for 'total'
            var data = data.filter(function(d) {
                return d.year != 'total';
            });

            data.forEach(function(d) {
                d.year = d.year;
                d.mails = d.mails;
            });

            x.domain(d3.extent(data, function(d) {
                return d.year;
            }));
            y.domain(d3.extent(data, function(d) {
                return d.mails;
            }));

            svg1.append("g")
                .attr("class", "x axis")
                .attr("transform", "translate(0," + height + ")")
                .call(xAxis1);

            svg1.append("g")
                .attr("class", "y axis")
                .call(yAxis1)
                .append("text")
                .attr("transform", "rotate(-90)")
                .attr("y", 6)
                .attr("dy", ".71em")
                .style("text-anchor", "end")
                .text("Number of emails");

            svg1.append("path")
                .datum(data)
                .attr("class", "line")
                .attr("d", line1);
        });

        // Second graph : Volume of X-Reference setted
        var xAxis2 = d3.svg.axis()
            .scale(x)
            .orient("bottom");

        var yAxis2 = d3.svg.axis()
            .scale(y)
            .orient("left");

        var line2 = d3.svg.line()
            .x(function(d) {
                return x(d.year);
            })
            .y(function(d) {
                return y(d.xreference);
            });

        var svg2 = d3.select("#volumeOfXreferenceSetted").append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        d3.json("data.json", function(error, data) {
            // Remove values for 'total'
            var data = data.filter(function(d) {
                return d.year != 'total';
            });

            data.forEach(function(d) {
                d.year = d.year;
                d.xreference = d.xreference;
            });

            x.domain(d3.extent(data, function(d) {
                return d.year;
            }));
            y.domain(d3.extent(data, function(d) {
                return d.xreference;
            }));

            svg2.append("g")
                .attr("class", "x axis")
                .attr("transform", "translate(0," + height + ")")
                .call(xAxis2);

            svg2.append("g")
                .attr("class", "y axis")
                .call(yAxis2)
                .append("text")
                .attr("transform", "rotate(-90)")
                .attr("y", 6)
                .attr("dy", ".71em")
                .style("text-anchor", "end")
                .text("Number of emails");

            svg2.append("path")
                .datum(data)
                .attr("class", "line")
                .attr("d", line2);
        });

        // Third graph : Volume of X-Reference setted by months in 2001
        var color = d3.scale.category10();

        var xAxis3 = d3.svg.axis()
            .scale(x)
            .orient("bottom");

        var yAxis3 = d3.svg.axis()
            .scale(y)
            .orient("left");

        var line3 = d3.svg.line()
            .x(function(d) {
                return x(d.month);
            })
            .y(function(d) {
                return y(d.value);  
            });

        var svg3 = d3.select("#volumeOfXreferenceSettedByMonths").append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
          .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        d3.json("data.json", function(error, data) {
          // Filter on year 2001
          var data = data.filter(function(d) {
            return d.year == '2001';
          })[0].months;

          color.domain(d3.keys(data[0]).filter(function(key) { return (key == 'mails' || key == 'xreference' || key == 'xreferencepercent'); }));

          var cities = color.domain().map(function(attr) {
            return {
              attr: attr,
              values: data.map(function(d) {
                return {month: d.month, value: d[attr]};
              })
            };
          });

          x.domain([1, 12]);

          y.domain([
            d3.min(cities, function(d) { return d3.min(d.values, function(d) { return d.value; }); }),
            d3.max(cities, function(d) { return d3.max(d.values, function(d) { return d.value; }); })
          ]);       

          svg3.append("g")
              .attr("class", "x axis")
              .attr("transform", "translate(0," + height + ")")
              .call(xAxis3);

          svg3.append("g")
              .attr("class", "y axis")
              .call(yAxis3)
            .append("text")
              .attr("transform", "rotate(-90)")
              .attr("y", 6)
              .attr("dy", ".71em")
              .style("text-anchor", "end")
              .text("Volume");

          var city = svg3.selectAll(".city")
              .data(cities)
            .enter().append("g")
              .attr("class", "city");

          city.append("path")
              .attr("class", "line")
              .attr("d", function(d) { return line3(d.values); })
              .style("stroke", function(d) { return color(d.attr); });

          city.append("text")
              .datum(function(d) { return {name: d.attr, value: d.values[d.values.length - 1]}; })
              .attr("transform", function(d) { return "translate(" + x(d.value.month) + "," + y(d.value.value) + ")"; })
              .attr("x", 3)
              .attr("dy", ".35em")
              .text(function(d) { return d.attr; });
        });

        // Fifth graph : Volume of threads launched
        var xAxis5 = d3.svg.axis()
            .scale(x)
            .orient("bottom");

        var yAxis5 = d3.svg.axis()
            .scale(y)
            .orient("left");

        var line5 = d3.svg.line()
            .x(function(d) {
                return x(d.year);
            })
            .y(function(d) {
                return y(d.threads);
            });

        var svg5 = d3.select("#volumeOfThreadsLaunched").append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
            .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        d3.json("data.json", function(error, data) {
            // Remove values for 'total'
            var data = data.filter(function(d) {
                return d.year != 'total';
            });

            data.forEach(function(d) {
                d.year = d.year;
                d.threads = d.threads;
            });

            x.domain(d3.extent(data, function(d) {
                return d.year;
            }));
            y.domain(d3.extent(data, function(d) {
                return d.threads;
            }));

            svg5.append("g")
                .attr("class", "x axis")
                .attr("transform", "translate(0," + height + ")")
                .call(xAxis5);

            svg5.append("g")
                .attr("class", "y axis")
                .call(yAxis5)
                .append("text")
                .attr("transform", "rotate(-90)")
                .attr("y", 6)
                .attr("dy", ".71em")
                .style("text-anchor", "end")
                .text("Number of emails");

            svg5.append("path")
                .datum(data)
                .attr("class", "line")
                .attr("d", line5);
        });
    }
);