/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class is used in Namesystem's web server to do login on namenode.
 */
public class LoginServlet extends DfsServlet {
  /** for java.io.Serializable */
  private static final long serialVersionUID = 1L;

  /** Handle login request */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response
      ) throws IOException {
    final Map<String,String[]> pmap = request.getParameterMap();
    final PrintWriter out = response.getWriter();
    final InetAddress remoteAddress = 
      InetAddress.getByName(request.getRemoteAddr());
    final ServletContext context = getServletContext();    
    final Configuration conf = NameNodeHttpServer.getConfFromContext(context);

    final UserGroupInformation ugi = getUGI(request, conf);
    try {
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          try {
            String userName = pmap.get("user")[0];
            String password = pmap.get("password")[0];
            boolean passed = UserGroupInformation.isAuthorized(userName, password);
            if (passed) {
              out.println("pass");
            } else {
              out.println("deny");
            }
          } catch (Exception e) {
            out.println("deny");
          }
           return null;
        }
      });
    } catch (InterruptedException e) {
      response.sendError(400, e.getMessage());
    }
  }
}
