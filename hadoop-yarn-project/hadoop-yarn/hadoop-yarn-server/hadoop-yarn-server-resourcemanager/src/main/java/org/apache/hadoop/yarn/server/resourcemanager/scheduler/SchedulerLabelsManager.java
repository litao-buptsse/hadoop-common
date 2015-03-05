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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

@Private
public class SchedulerLabelsManager {
  private static final Log LOG = LogFactory.getLog(SchedulerLabelsManager.class);
  
  Map<String, SchedulerLabel> labels = new ConcurrentHashMap<String, SchedulerLabel>();
  
  public synchronized void addNode(RMNode node) {
    if (node.getLabels() == null) {
      return;
    }

    for (String label : node.getLabels()) {
      SchedulerLabel schedulerLabel = labels.get(label);
      if (schedulerLabel ==  null) {
        schedulerLabel = new SchedulerLabel();
        labels.put(label, schedulerLabel);
      }
      schedulerLabel.addNode();
      
      LOG.info("Added label " + label + " for node " + node.getNodeID());
    }
  }
  
  public synchronized void removeNode(RMNode node) {
    if (node.getLabels() == null) {
      return;
    }


    for (String label : node.getLabels()) {
      SchedulerLabel schedulerLabel = labels.get(label);
      schedulerLabel.removeNode();  // This cannot be null, ever
      
      // Remove label if it's no longer valid, also inform applications
      if (!schedulerLabel.isValid()) {
        for (SchedulerApplicationAttempt application : schedulerLabel.getApplications()) {
          LOG.info("Label: " + label + " is invalid after removing node " + 
              node.getNodeID() + ", informing application: " + 
              application.getApplicationAttemptId());
          application.addInvalidLabel(label);
        }
        labels.remove(label);
      }
      
      LOG.info("Removed label " + label + " emanating from node " + 
          node.getNodeID());
    }
  }
  
  public void addLabelToApplication(
      String label, SchedulerApplicationAttempt application) {
    SchedulerLabel schedulerLabel = labels.get(label);

    // Sanity check: Is the label valid?
    if (schedulerLabel == null || !schedulerLabel.isValid()) {
      LOG.info("Application " + 
          application.getApplicationAttemptId() + 
           " asking for invalid label: " + label);
      application.addInvalidLabel(label);
      return;
    }
    
    schedulerLabel.addLabelToApplication(application);
  }
  
  public void removeLabelFromApplication(
      String label, SchedulerApplicationAttempt application) {
    SchedulerLabel schedulerLabel = labels.get(label);
    if (schedulerLabel != null) {
      schedulerLabel.removeLabelFromApplication(application);
    }
  }

  /**
   * Information about a label in the system.
   * 
   * <code>SchedulerLabel</code> tracks a validity of the <em>label</em> and
   * also the <code>applications</code> which are using this <em>label</em>.
   */
  @Private
  static class SchedulerLabel {

    int refCount;
    Set<SchedulerApplicationAttempt> applications = 
        new ConcurrentSkipListSet<SchedulerApplicationAttempt>();
    
    public synchronized void addNode() {
      ++refCount;
    }
    
    public synchronized void removeNode() {
      --refCount;
    }
    
    public synchronized boolean isValid() {
      return refCount > 0;
    }
    
    public synchronized void addLabelToApplication(
        SchedulerApplicationAttempt application) {
      applications.add(application);
    }
    
    public synchronized void removeLabelFromApplication(
        SchedulerApplicationAttempt application) {
      applications.remove(application);
    }
    
    public synchronized Collection<SchedulerApplicationAttempt> getApplications() {
      return applications;
    }
  }

}
