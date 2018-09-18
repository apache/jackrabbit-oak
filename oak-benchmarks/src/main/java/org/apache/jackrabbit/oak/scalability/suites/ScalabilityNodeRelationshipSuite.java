/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.scalability.suites;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.ImmutableList;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.benchmark.util.OakIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.scalability.util.NodeTypeUtils;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Splitter;
import com.google.common.base.StandardSystemProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * The suite test will incrementally increase the load and execute searches.
 * Each test run thus adds nodes and executes different benchmarks. This way we measure time
 * taken for benchmark execution.
 *
 * <p>
 * The following system JVM properties can be defined to configure the suite.
 * <ul>
 * <li>
 *     <code>nodeLevels</code> - Comma separated string property that governs the number of number of
 *     different node relationships in the following order:
 *      <ul>
 *          <li>Users</li>
 *          <li>Groups</li>
 *          <li>User Relationships</li>
 *          <li>Activities</li>
 *      </ul>
 *
 *     Defaults to 10,5,2,1.
 * </li>
 * </ul>
 *
 */
public class ScalabilityNodeRelationshipSuite extends ScalabilityNodeSuite {
    private static final Logger LOG =
        LoggerFactory.getLogger(ScalabilityNodeRelationshipSuite.class);

    public static final String CUSTOM_ACT_NODE_TYPE = "ActivityType";

    public static final String CUSTOM_REL_NODE_TYPE = "RelationshipType";

    public static final String ACTIVITIES = "Activities";

    public static final String RELATIONSHIPS = "Relationships";

    /**
     * Node properties
     */
    public static final String CTX_USER = "User";
    public static final String CTX_GROUP = "Group";
    public static final String CREATED = "jcr:created";
    public static final String SOURCE_ID = "sourceId";
    public static final String TARGET_ID = "targetId";
    public static final String ACTION = "action";
    public static final String SOURCE = "source";
    public static final String OBJECT = "object";
    public static final String OBJECT_ID = "objectId";
    public static final String TARGET = "target";

    protected static final List<String> NODE_LEVELS = Splitter.on(",").trimResults()
        .omitEmptyStrings().splitToList(System.getProperty("nodeLevels", "10,5,2,1"));

    protected static final List<String> NODE_LEVELS_DEFAULT = ImmutableList.of("10","5","2","1");

    private static final int NUM_USERS =
        (NODE_LEVELS.size() >= 1 ? Integer.parseInt(NODE_LEVELS.get(0)) : Integer.parseInt(NODE_LEVELS_DEFAULT.get(0)));

    private static final int NUM_GROUPS =
        (NODE_LEVELS.size() >= 2 ? Integer.parseInt(NODE_LEVELS.get(1)) : Integer.parseInt(NODE_LEVELS_DEFAULT.get(1)));

    private static final int NUM_RELATIONSHIPS =
        (NODE_LEVELS.size() >= 3 ? Integer.parseInt(NODE_LEVELS.get(2)) : Integer.parseInt(NODE_LEVELS_DEFAULT.get(2)));

    private static final int NUM_ACTIVITIES =
        (NODE_LEVELS.size() >= 4 ? Integer.parseInt(NODE_LEVELS.get(3)) : Integer.parseInt(NODE_LEVELS_DEFAULT.get(3)));


    private static final long BUCKET_SIZE = 100;

    private static final List<String> actions = Lists
        .newArrayList("act1", "act2", "act3", "act4", "act5", "act6", "act7", "act8", "act9",
            "act10");
    private static final List<String> objects = Lists
        .newArrayList("obj1", "obj2", "obj3", "obj4", "obj5", "obj6", "obj7", "obj8", "obj9",
            "obj10");

    private final Random random = new Random(29);

    private List<Authorizable> users;
    private List<Authorizable> groups;

    public ScalabilityNodeRelationshipSuite(Boolean storageEnabled) {
        super(storageEnabled);
    }

    @Override
    protected void beforeSuite() throws Exception {
        Session session = loginWriter();
        Node root = session.getRootNode();
        root.addNode(ROOT_NODE_NAME);
        session.save();

        users = Lists.newArrayList();
        groups = Lists.newArrayList();

        if (CUSTOM_TYPE) {
            NodeTypeUtils.createNodeType(session, CUSTOM_ACT_NODE_TYPE,
                new String[] {TITLE_PROP, CREATED, ACTION, SOURCE_ID},
                new int[] {PropertyType.STRING, PropertyType.DATE, PropertyType.STRING,
                    PropertyType.STRING}, new String[0],
                new String[] {NodeTypeConstants.NT_OAK_UNSTRUCTURED}, null, false);
            NodeTypeUtils.createNodeType(session, CUSTOM_REL_NODE_TYPE,
                new String[] {CREATED, SOURCE_ID, TARGET_ID},
                new int[] {PropertyType.DATE, PropertyType.STRING, PropertyType.STRING},
                new String[0], null, null, false);
            nodeTypes.add(CUSTOM_ACT_NODE_TYPE);
            nodeTypes.add(CUSTOM_REL_NODE_TYPE);
        }

        if (INDEX) {
            createIndexes(session);
        }
    }

    protected void createIndexes(Session session) throws RepositoryException {
        Map<String, Map<String, String>> orderedMap = Maps.newHashMap();
        String persistencePath = "";

        // define indexes on properties
        switch (INDEX_TYPE) {
            case PROPERTY:
                OakIndexUtils.propertyIndexDefinition(session, "customIndexActivity",
                    new String[] {SOURCE_ID}, false,
                    (!CUSTOM_TYPE ? new String[0] : new String[] {CUSTOM_ACT_NODE_TYPE}));
                OakIndexUtils.propertyIndexDefinition(session, "customIndexRelationship",
                    new String[] {SOURCE_ID}, false,
                    (!CUSTOM_TYPE ? new String[0] : new String[] {CUSTOM_REL_NODE_TYPE}));
                break;
            // define ordered indexes on properties
            case ORDERED:
                OakIndexUtils.orderedIndexDefinition(session, "customIndexActivity", ASYNC_INDEX,
                    new String[] {CREATED}, false,
                    (!CUSTOM_TYPE ? new String[0] : new String[] {CUSTOM_ACT_NODE_TYPE}),
                    OrderedIndex.OrderDirection.DESC.getDirection());
                OakIndexUtils
                    .orderedIndexDefinition(session, "customIndexRelationship", ASYNC_INDEX,
                        new String[] {CREATED}, false,
                        (!CUSTOM_TYPE ? new String[0] : new String[] {CUSTOM_REL_NODE_TYPE}),
                        OrderedIndex.OrderDirection.DESC.getDirection());
                break;
            // define lucene index on properties
            case LUCENE_FILE:
                persistencePath =
                    "target" + StandardSystemProperty.FILE_SEPARATOR.value() + "lucene" + String
                        .valueOf(System.currentTimeMillis());
                OakIndexUtils.luceneIndexDefinition(session, "customIndexActivity", ASYNC_INDEX,
                    new String[] {SOURCE_ID, CREATED},
                    new String[] {PropertyType.TYPENAME_STRING, PropertyType.TYPENAME_DATE},
                    orderedMap, persistencePath);
                break;
            case LUCENE_FILE_DOC:
                persistencePath =
                    "target" + StandardSystemProperty.FILE_SEPARATOR.value() + "lucene" + String
                        .valueOf(System.currentTimeMillis());
            case LUCENE_DOC:
                Map<String, String> propMap = Maps.newHashMap();
                propMap.put(LuceneIndexConstants.PROP_TYPE, PropertyType.TYPENAME_DATE);
                orderedMap.put(CREATED, propMap);
            case LUCENE:
                OakIndexUtils.luceneIndexDefinition(session, "customIndexActivity", ASYNC_INDEX,
                    new String[] {SOURCE_ID, CREATED},
                    new String[] {PropertyType.TYPENAME_STRING, PropertyType.TYPENAME_DATE},
                    orderedMap, persistencePath);
                break;
        }
    }

    /**
     * Executes before each test run
     */
    @Override
    public void beforeIteration(ExecutionContext context) throws RepositoryException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Started beforeIteration()");
        }

        // Contextualize the node types being used
        if (nodeTypes != null && !nodeTypes.isEmpty()) {
            context.getMap().put(CTX_ACT_NODE_TYPE_PROP, nodeTypes.get(0));
            context.getMap().put(CTX_REL_NODE_TYPE_PROP, nodeTypes.get(1));
        }

        Session session = loginWriter();
        UserManager userMgr = ((JackrabbitSession) session).getUserManager();

        context.getMap().put("PREV_ITER_USERS", users.size());

        // Create Users and Groups based on the load for this iteration (cumulatively)
        // Add users
        for (int idx = 0; idx < NUM_USERS * context.getIncrement(); idx++) {
            String name = String.valueOf((char) (random.nextInt(26) + 'a')) + CTX_USER + context
                .getIncrement() + "_" + idx;
            User user = userMgr.createUser(name, name);
            LOG.debug("User created : " + name);
            users.add(user);
        }

        // Add groups and include random number of members
        for (int idx = 0; idx < NUM_GROUPS * context.getIncrement(); idx++) {
            String name = String.valueOf((char) (random.nextInt(26) + 'a')) + CTX_GROUP + context
                .getIncrement() + idx;
            Group group = userMgr.createGroup(name);
            groups.add(group);
            int groupMembers = random.nextInt(users.size());
            for (int i = 0; i < groupMembers; i++) {
                group.addMember(users.get(random.nextInt(users.size())));
            }
        }
        session.save();
        // create the load for this iteration
        createLoad(context);
        long loadFinish = System.currentTimeMillis();

        context.getMap().put(CTX_ROOT_NODE_NAME_PROP, ROOT_NODE_NAME);
        context.getMap().put(CTX_USER, users);
        context.getMap().put(CTX_GROUP, groups);

        waitBeforeIterationFinish(loadFinish);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished beforeIteration()");
        }
    }

    @Override
    protected Writer getWriter(ExecutionContext context,
        SynchronizedDescriptiveStatistics writeStats, int idx) throws RepositoryException {
        int numUsers = (context.getIncrement() * NUM_USERS) / LOADERS;
        return new ActivityWriter((context.getIncrement() + "-" + idx), numUsers, idx * numUsers,
            writeStats);
    }

    /**
     * The users are created with the nomenclature {@code [a-z]User<INCREMENT>_<ID>}
     *
     * <p>
     *
     * Creates a node hierarchy similar to the node structure below.
     * Here for example aUser0_1 and cUser0_5 are 2 users and aUser0_1 has a relationship structure to user cUser0_5.
     *
     * <pre>
     * {@code
     * /home
     *  /a
     *      /aUser0_1
     *          /Relationships
     *              /cUser0_5
     *                  jcr:primaryType : <oak:Unstructured|descendantType|nt:unstructured>
     *                  jcr:created : <DATE>
     *                  sourceId : aUser0_1
     *                  targetId : cUser0_5
     *          /Activities
     *             /2015
     *                 /06
     *                     /03
     *                         /@1
     *                             /<UUID>
     *                                 jcr:primaryType : <oak:Unstructured|descendantType|nt:unstructured>
     *                                 title : <sourceId targetId>
     *                                 action : <act*>
     *                                 sourceId : aUser0_1
     *                                 /source
     *                                     sourceId : aUser0_1
     *                                 /object
     *                                     objectId: <obj*>
     *                                 /target
     *                                     targetId: cUser0_5
     * }
     * </pre>
     * </p>
     */
    class ActivityWriter extends Writer {
        private int startIdx;

        ActivityWriter(String id, int numUsers, int startIdx,
            SynchronizedDescriptiveStatistics writeStats) throws RepositoryException {
            super(id, numUsers, writeStats);
            this.startIdx = startIdx;
        }

        @Override
        public void run() {
            try {
                int idx = startIdx;
                while (idx < (maxAssets + startIdx)) {
                    session.refresh(false);

                    // Current User
                    int userIdx = (Integer) context.getMap().get("PREV_ITER_USERS") + idx;
                    Authorizable user = users.get(userIdx);

                    Node activitiesParentNode = JcrUtils
                        .getOrAddNode(session.getNode(user.getPath()), ACTIVITIES,
                            NodeTypeConstants.NT_OAK_UNSTRUCTURED);
                    Node relationshipsParentNode = JcrUtils
                        .getOrAddNode(session.getNode(user.getPath()), RELATIONSHIPS,
                            NodeTypeConstants.NT_OAK_UNSTRUCTURED);
                    createRelationships(user, relationshipsParentNode, activitiesParentNode);
                    createActivities(user, activitiesParentNode);

                    if ((counter + 1) % 100 == 0) {
                        LOG.info("Thread " + id + " - Processed Users : " + (counter + 1));
                    }
                    idx++;
                    counter++;
                }
            } catch (Exception e) {
                LOG.error("Exception in load creation ", e);
            }
        }

        /**
         * Create activities for a use. The number of activities is governed by
         * {# NODE_LEVELS.get(3)}
         *
         * @param user                 the user for who activities are to be created
         * @param activitiesParentNode the parent node for all the user activities
         * @throws RepositoryException
         */
        private void createActivities(Authorizable user, Node activitiesParentNode)
            throws RepositoryException {
            for (int i = 0; i < NUM_ACTIVITIES; i++) {
                timer.start();

                createActivity(activitiesParentNode, user.getID() + " " + i,
                    actions.get(random.nextInt(actions.size())), user.getID(),
                    objects.get(random.nextInt(objects.size())),
                    objects.get(random.nextInt(objects.size())));

                session.save();

                // Record time taken for creation
                timer.stop();
            }
        }

        private void createActivity(Node activitiesParentNode, String title,
                                    String action, String source, String object, String target) throws RepositoryException {
            Node activityNode = getActivityParentNode(activitiesParentNode);

            Map<String, String> activityMap = Maps.newHashMap();
            activityMap.put(TITLE_PROP, title);
            activityMap.put(ACTION, action);
            activityMap.put(SOURCE_ID, source);
            activityMap.put(OBJECT_ID, object);
            activityMap.put(TARGET_ID, target);

            createActivityNode(activityNode, activityMap);
        }

        /**
         * Creates the activity node structure.
         */
        private void createActivityNode(Node activityParent, Map<String, String> props)
            throws RepositoryException {
            activityParent.setProperty(TITLE_PROP, props.get(TITLE_PROP));
            activityParent.setProperty(CREATED, generateDate());
            activityParent.setProperty(ACTION, props.get(ACTION));
            activityParent.setProperty(SOURCE_ID, props.get(SOURCE_ID));
            Node sourceNode = JcrUtils
                .getOrAddNode(activityParent, SOURCE, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            sourceNode.setProperty(SOURCE_ID, props.get(SOURCE_ID));

            Node objNode = JcrUtils
                .getOrAddNode(activityParent, OBJECT, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            objNode.setProperty(OBJECT_ID, props.get(OBJECT_ID));

            Node targetNode = JcrUtils
                .getOrAddNode(activityParent, TARGET, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            targetNode.setProperty(TARGET_ID, props.get(TARGET_ID));

            LOG.debug(
                "Activity created for User : " + props.get(SOURCE_ID) + " " + activityParent.getPath());
        }

        /**
         * Creates bucketed parent node for the activity.
         */
        private Node getActivityParentNode(Node activitiesParentNode) throws RepositoryException {
            Calendar c = Calendar.getInstance();
            Node yearNode = JcrUtils
                .getOrAddNode(activitiesParentNode, String.valueOf(c.get(Calendar.YEAR)),
                    NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            String month = String.valueOf(c.get(Calendar.MONTH) + 1);
            month = month.length() > 1 ? month : "0" + month;
            Node monthNode =
                JcrUtils.getOrAddNode(yearNode, month, NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            String day = String.valueOf(c.get(Calendar.DATE));
            day = day.length() > 1 ? day : "0" + day;
            Node dayNode =
                JcrUtils.getOrAddNode(monthNode, day, NodeTypeConstants.NT_OAK_UNSTRUCTURED);

            // find bucket
            Node parentNode = dayNode;
            NodeIterator iterator = dayNode.getNodes();
            long size = iterator.getSize();
            if (size < 0 || size > BUCKET_SIZE) {
                size = 0;
                int maxNum = -1;
                while (iterator.hasNext()) {
                    size++;
                    Node child = iterator.nextNode();
                    String name = child.getName();
                    if (name.charAt(0) == '@') {
                        int buckNum = Integer.parseInt(name.substring(1));
                        if (buckNum > maxNum) {
                            maxNum = buckNum;
                            parentNode = child;
                        }
                    }
                }
                if (size > BUCKET_SIZE) {
                    // check if last bucket has enough space
                    if (maxNum < 0 || numChildNodes(parentNode) >= BUCKET_SIZE) {
                        parentNode = dayNode.addNode("@" + String.valueOf(maxNum + 1),
                            NodeTypeConstants.NT_OAK_UNSTRUCTURED);
                    }
                }
            }
            // create activity node
            return JcrUtils
                .getOrCreateUniqueByPath(parentNode, UUID.randomUUID().toString(), getType(0));
        }

        private long numChildNodes(Node node) throws RepositoryException {
            NodeIterator iterator = node.getNodes();
            if (iterator.getSize() >= 0) {
                return iterator.getSize();
            } else {
                int num = 0;
                while (iterator.hasNext() && num < BUCKET_SIZE) {
                    iterator.nextNode();
                    num++;
                }
                return num;
            }
        }

        /**
         * Create relationships to other users. The number of relationships is governed by
         * {# NODE_LEVELS.get(2)}
         *
         * @param user                    the source user of the relationships
         * @param relationshipsParentNode the node where the relationships are recorded  @throws
         *                                RepositoryException
         * @param activitiesParentNode the parent node for all the user activities
         */
        private void createRelationships(Authorizable user, Node relationshipsParentNode,
            Node activitiesParentNode) throws RepositoryException {
            List<Integer> usersIdx = Lists.newArrayList();
            for (int count = 0; count < users.size(); count++) {
                usersIdx.add(count);
            }

            for (int i = 0; i < NUM_RELATIONSHIPS; i++) {
                if (usersIdx.size() > 0) {
                    String otherUser =
                        users.get(usersIdx.remove(random.nextInt(usersIdx.size()))).getID();
                    timer.start();

                    String nameHint = Text.getName(otherUser);
                    Node rNode = relationshipsParentNode.addNode(nameHint, getType(1));
                    rNode.setProperty(CREATED, generateDate());
                    rNode.setProperty(SOURCE_ID, user.getID());
                    rNode.setProperty(TARGET_ID, otherUser);

                    LOG.debug(
                        "Relationship created for User : " + user.getID() + " " + rNode.getPath());
                    createActivity(activitiesParentNode, user.getID() + " " + otherUser,
                        actions.get(random.nextInt(actions.size())), user.getID(),
                        objects.get(random.nextInt(objects.size())), otherUser);

                    session.save();

                    timer.stop();
                }
            }
        }

        /**
         * Order of precedence is custom type or oak:Unstructured
         *
         * @return the type
         * @throws RepositoryException the repository exception
         */
        protected String getType(int typeIdx) throws RepositoryException {
            String typeOfNode = (typeIdx == 0 ? CTX_ACT_NODE_TYPE_PROP : CTX_REL_NODE_TYPE_PROP);

            String type = NodeTypeConstants.NT_OAK_UNSTRUCTURED;
            if (context.getMap().containsKey(typeOfNode)) {
                type = (String) context.getMap().get(typeOfNode);
            } else {
                context.getMap().put(typeOfNode, type);
            }
            return type;
        }
    }
}

