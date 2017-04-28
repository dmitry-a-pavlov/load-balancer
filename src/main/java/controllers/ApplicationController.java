/**
 * Copyright (C) 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package controllers;

import ninja.Result;
import ninja.Results;
import ninja.params.PathParam;
import system.SysCluster;

import com.google.inject.Inject;
import com.google.inject.Singleton;


@Singleton
public class ApplicationController {
	
	private SysCluster cluster;
	
	@Inject
	public ApplicationController(SysCluster cluster) {
		this.cluster = cluster;
	}


    public Result index() {
    	Result result = Results.html();
    	result.render("clusterLoad", cluster.toString());
        return result;
    }
    
    public Result helloWorldJson() {
        
        SimplePojo simplePojo = new SimplePojo();
        simplePojo.content = "Hello World! Hello Json!";

        return Results.json().render(simplePojo);

    }
    
    public static class SimplePojo {

        public String content;
        
    }
    
    public Result checkClusterHealth( @PathParam("type") String type ) {
    	int loadType =  type == null || type.length() == 0? -1: Integer.parseInt(type);

    	return Results.json().render(cluster.checkClusterHealth(loadType));

    }

    public Result getStatistics() {
    	return Results.json().render(cluster.getStatistics());
    }

}
