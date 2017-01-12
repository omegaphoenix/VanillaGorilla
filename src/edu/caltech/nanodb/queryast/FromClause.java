package edu.caltech.nanodb.queryast;


import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.InsertCommand;
import edu.caltech.nanodb.commands.SelectCommand;
import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.ColumnValue;
import edu.caltech.nanodb.expressions.CompareOperator;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.FunctionCall;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.TableManager;


/**
 * This class represents a hierarchy of one or more base and derived relations
 * that produce the rows considered by <tt>SELECT</tt> clauses.
 * <p>
 * The "join condition type" specified in the {@link #condType} field
 * indicates the way that the join condition was specified in the original SQL
 * expression.  This value may be {@code null} if the join specified no
 * condition, which would occur with these SQL expressions:
 * <ul>
 *   <li>SELECT * FROM t1, t2 WHERE t1.a = t2.a;</li>
 *   <li>SELECT * FROM t1 JOIN t2 WHERE t1.a = t2.a;</li>
 * </ul>
 * <p>
 * If <tt>condType</tt> is set to <tt>JoinConditionType.JOIN_ON_EXPR</tt> then
 * the join condition was specified in an <tt>ON</tt> clause, such as:
 * <ul>
 *   <li>SELECT * FROM t1 JOIN t2 ON t1.a = t2.a;</li>
 * </ul>
 * The <tt>ON</tt> predicate is available via the {@link #getOnExpression}
 * method.
 * <p>
 * Finally, if the <tt>condType</tt> is either
 * <tt>JoinConditionType.JOIN_USING</tt> or
 * <tt>JoinConditionType.NATURAL_JOIN</tt> then the join condition was
 * implicitly specified via a SQL <tt>USING</tt> clause, or via a natural join,
 * such as:
 * <ul>
 *   <li>SELECT * FROM t1 JOIN t2 USING (a);</li>
 *   <li>SELECT * FROM t1 NATURAL JOIN t2;</li>
 * </ul>
 * In these cases the join predicate must be generated from examining the
 * schemas of the participating tables.  The generated predicate is only
 * available <em>after</em> the {@link #computeSchema} method has been called (which
 * is indirectly invoked by the {@link SelectCommand} and {@link InsertCommand}
 * classes before planning and executing a query), and is available via the
 * {@link #getComputedJoinExpr()} method.
 *
 * @see SelectClause
 */
public class FromClause {

    /**
     * This enumeration specifies the overall type of the <tt>FROM</tt>
     * clause.  Since different kinds of FROM clauses have different
     * characteristics, we drive some of the logic in this class with this
     * enumeration.
     */
    public enum ClauseType {
        /** This clause is a simple table (a "base relation"). */
        BASE_TABLE,

        /**
         * This clause is a derived relation specified as a named
         * <tt>SELECT</tt> subquery.
         */
        SELECT_SUBQUERY,

        /**
         * This clause is a derived relation specified as a join of two
         * relations.
         */
        JOIN_EXPR,

        /** This clause is a table-returning function. */
        TABLE_FUNCTION
    }


    /**
     * For FROM clauses that contain join expressions, this enumeration
     * specifies the kind of join-condition for each join expression.
     */
    public enum JoinConditionType {
        /**
         * Perform a natural join, which implicitly specifies that values in all
         * shared columns must be equal.
         */
        NATURAL_JOIN,

        /**
         * The join clause specifies a USING clause, which explicitly lists the
         * shared columns whose values must be equal.
         */
        JOIN_USING,

        /**
         * The join clause specifies an ON clause with an expression that must
         * evaluate to true.
         */
        JOIN_ON_EXPR
    }


    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(FromClause.class);


    /**
     * The kind of FROM clause this object represents, either a base table, a
     * derived table (a nested SELECT), or a join expression.
     */
    private ClauseType clauseType;


    /**
     * If the <tt>FROM</tt> clause specifies a base table or a table-returning
     * function, then this field holds the table or function's name.  This
     * field will only be set to a non-{@code null} value if
     * {@link #clauseType} is set to {@link ClauseType#BASE_TABLE} or
     * {@link ClauseType#TABLE_FUNCTION}.
     */
    private String tableName;


    /**
     * If the <tt>FROM</tt> clause is a table-returning function then this
     * field holds the arguments passed to the table function.  This field
     * will only be set to a non-{@code null} value if {@link #clauseType} is
     * set to {@link ClauseType#TABLE_FUNCTION}.
     */
    private List<Expression> args = null;


    /**
     * If the <tt>FROM</tt> clause specifies a nested <tt>SELECT</tt> command
     * with an alias, this field holds the nested query.  This field will
     * only be set to a non-<code>null</code> value if {@link #clauseType}
     * is set to {@link ClauseType#SELECT_SUBQUERY}.
     */
    private SelectClause derivedTable = null;


    /**
     * If this <tt>FROM</tt> clause is a base table or a derived table, then
     * this field stores the alias for the table.  The alias is optional for
     * base tables, but it is required for derived tables.
     */
    private String aliasName = null;


    /**
     * If this <tt>FROM</tt> clause is a join expression then this references
     * the left child of the join.  This field will only be set to a
     * non-<code>null</code> value if {@link #clauseType}
     * is set to {@link ClauseType#JOIN_EXPR}.
     */
    private FromClause leftChild;


    /**
     * If this <tt>FROM</tt> clause is a join expression then this references
     * the right child of the join.  This field will only be set to a
     * non-<code>null</code> value if {@link #clauseType}
     * is set to {@link ClauseType#JOIN_EXPR}.
     */
    private FromClause rightChild;


    /**
     * If this object represents a join expression, this field specifies
     * the type of join to use, whether it is an inner join, some kind of
     * outer join, or a cross join.
     */
    private JoinType joinType;


    /**
     * If this object represents a join expression, this field specifies the
     * type of condition used in the join; either a natural join, a join with
     * an <tt>ON</tt> expression, or a join with a <tt>USING</tt> clause.
     */
    private JoinConditionType condType;


    private List<String> joinUsingNames = new ArrayList<>();


    /** The expression used for joining tables in this <tt>FROM</tt> clause. */
    private Expression joinOnExpr = null;


    /**
     * This value is filled in with the schema of the data that this
     * <tt>FROM</tt> clause produces, when the {@link #computeSchema} method
     * is called.  This is done when a SQL statement is being prepared for
     * execution, and is typically kicked off by the
     * {@link SelectClause#computeSchema} method, which in turn prepares the
     * from-clause objects.
     */
    private Schema computedSchema = null;


    /**
     * For <tt>FROM</tt> clauses that either directly or indirectly specify a
     * join condition, this value is filled in with the join condition to apply,
     * when the {@link #computeSchema} method is called.  This is done when a SQL
     * statement is being prepared for execution, and is typically kicked off by
     * the {@link SelectClause#computeSchema} method, which in turn prepares the
     * from-clause objects.
     */
    private Expression computedJoinExpr = null;


    /**
     * In cases of <tt>NATURAL</tt> joins or joins with a <tt>USING</tt> clause,
     * this value is filled in with {@link SelectValue} objects that can be used
     * to perform a suitable project to eliminate duplicate columns.  If no
     * duplicate columns are to be eliminated, this value will be <tt>null</tt>.
     * This value is populated when the {@link #computeSchema} method is called, and
     * is typically kicked off by the {@link SelectClause#computeSchema} method,
     * which in turn prepares the from-clause objects.
     */
    private ArrayList<SelectValue> computedSelectValues = null;


    /**
     * Construct a new <tt>FROM</tt> clause of type {@link ClauseType#BASE_TABLE}.
     * The alias name may be <tt>null</tt> in the case of base-tables.
     *
     * @param tableName the name of the table being selected from
     * @param aliasName an optional alias used to refer to the table in the rest
     *        of the query
     */
    public FromClause(String tableName, String aliasName) {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        clauseType = ClauseType.BASE_TABLE;

        this.tableName = tableName;
        this.aliasName = aliasName;
    }


    public FromClause(String functionName, List<Expression> args,
                      String aliasName) {
        if (functionName == null)
            throw new IllegalArgumentException("functionName cannot be null");

        if (args == null)
            throw new IllegalArgumentException("args cannot be null");

        clauseType = ClauseType.TABLE_FUNCTION;

        this.tableName = functionName;
        this.aliasName = aliasName;
    }


    /**
     * Construct a new <tt>FROM</tt> clause of type
     * {@link FromClause.ClauseType#SELECT_SUBQUERY}.  The alias name must be
     * specified for derived tables.
     *
     * @param derivedTable the subquery that is referenced by this from-clause
     * @param aliasName the relation-name given for this subquery
     *
     * @throws NullPointerException if either <tt>derivedTable</tt> or
     *         <tt>aliasName</tt> is <tt>null</tt>
     */
    public FromClause(SelectClause derivedTable, String aliasName) {
        if (derivedTable == null)
            throw new NullPointerException("derivedTable cannot be null");

        if (aliasName == null) {
            throw new NullPointerException(
                "aliasName cannot be null for a derived table");
        }

        clauseType = ClauseType.SELECT_SUBQUERY;

        this.derivedTable = derivedTable;
        this.aliasName = aliasName;
    }


    /**
     * Construct a new <tt>FROM</tt> clause of type
     * {@link FromClause.ClauseType#JOIN_EXPR}.  The two sub-relations being
     * joined are specified as <tt>FromClause</tt> instances.
     *
     * @param left the left relation being joined
     * @param right the right relation being joined
     * @param type the kind of join being performed
     *
     * @throws NullPointerException if either sub-relation is <tt>null</tt>, or
     *         if the join-type is <tt>null</tt>.
     */
    public FromClause(FromClause left, FromClause right, JoinType type) {
        if (left == null)
            throw new NullPointerException("left cannot be null");

        if (right == null)
            throw new NullPointerException("right cannot be null");

        if (type == null)
            throw new NullPointerException("type cannot be null");

        clauseType = ClauseType.JOIN_EXPR;

        leftChild = left;
        rightChild = right;
        joinType = type;
    }



    /**
     * Returns the kind of <tt>FROM</tt> clause this object represents.
     *
     * @return the clause-type.  This value will never be <tt>null</tt>.
     */
    public ClauseType getClauseType() {
        return clauseType;
    }


    /**
     * Returns true if this from clause specifies a base table, as opposed
     * to a derived table or a join expression.
     *
     * @return <tt>true</tt> if this clause specifies a base table.
     */
    public boolean isBaseTable() {
        return (clauseType == ClauseType.BASE_TABLE);
    }


    /**
     * Returns true if this from clause specifies a derived table, as
     * opposed to a base table or a join expression.
     *
     * @return {@code true} if this clause is a derived table specified as a
     *         <tt>SELECT</tt> statement.
     */
    public boolean isDerivedTable() {
        return (clauseType == ClauseType.SELECT_SUBQUERY);
    }


    /**
     * Returns true if this from clause specifies a join expression, as opposed
     * to a base table or a join expression.
     *
     * @return {@code true} if this clause is a join expression.
     */
    public boolean isJoinExpr() {
        return (clauseType == ClauseType.JOIN_EXPR);
    }


    /**
     * Returns the table name. This value will be null if this is a derived table
     * or a join expression.
     *
     * @return the name of the table.
     *
     * @throws IllegalStateException if the from-clause's type is not a
     *         {@link ClauseType#BASE_TABLE}.
     */
    public String getTableName() {
        if (clauseType != ClauseType.BASE_TABLE) {
            throw new IllegalStateException("From-clause is a " + clauseType +
                " clause, not a BASE_TABLE.");
        }

        return tableName;
    }


    /**
     * Returns the select clause for a derived table. This value will be
     * null if this is not derived.
     *
     * @return the derived select clause
     *
     * @throws IllegalStateException if the from-clause's type is not a
     *         {@link ClauseType#SELECT_SUBQUERY}.
     */
    public SelectClause getSelectClause() {
        if (clauseType != ClauseType.SELECT_SUBQUERY) {
            throw new IllegalStateException("From-clause is a " + clauseType +
                " clause, not a SELECT_SUBQUERY.");
        }

        return derivedTable;
    }


    /**
     * If the from-clause is a join expression, this method returns the left
     * child-clause.
     *
     * @return the left child-clause of this join clause
     *
     * @throws IllegalStateException if this from-clause is not a join
     *         expression.
     */
    public FromClause getLeftChild() {
        if (clauseType != ClauseType.JOIN_EXPR) {
            throw new IllegalStateException("From-clause is a " + clauseType +
                " clause, not a JOIN_EXPR.");
        }

        return leftChild;
    }


    /**
     * If the from-clause is a join expression, this method returns the right
     * child-clause.
     *
     * @return the right child-clause of this join clause
     *
     * @throws IllegalStateException if this from-clause is not a join
     *         expression.
     */
    public FromClause getRightChild() {
        if (clauseType != ClauseType.JOIN_EXPR) {
            throw new IllegalStateException("From-clause is a " + clauseType +
                " clause, not a JOIN_EXPR.");
        }

        return rightChild;
    }


    /**
     * Returns the name of the relation that the results should go into. In the
     * case where an alias exists, this returns the alias. If an alias does not
     * exist, this must return the table name for the base table.
     *
     * @return the result relation name
     *
     * @throws IllegalStateException if the clause-type is a join expression,
     *         since that kind of from-clause doesn't have a result name.
     */
    public String getResultName() {
        // Join expressions aren't renamed; only tables and SELECT subqueries
        // can be given a name (in fact, SELECT subqueries require one).
        if (clauseType == ClauseType.JOIN_EXPR) {
            throw new IllegalStateException(clauseType +
                " clauses don't have a result name");
        }

        if (aliasName != null)
            return aliasName;

        return tableName;
    }


    public boolean isRenamed() {
        // Join expressions aren't renamed; only tables and SELECT subqueries
        // can be given a name (in fact, SELECT subqueries require one).
        if (clauseType == ClauseType.JOIN_EXPR)
            return false;

        return (aliasName != null);
    }


    /**
     * Returns the join type of a join clause.
     *
     * @return the join type
     *
     * @throws IllegalStateException if the clause-type is not a join clause.
     *         Only join clauses have a join type.
     */
    public JoinType getJoinType() {
        if (clauseType != ClauseType.JOIN_EXPR) {
            throw new IllegalStateException(clauseType +
                " clauses don't have a join type");
        }

        return joinType;
    }


    /**
     * Returns true if this join clause is an outer join, false otherwise.
     *
     * @return true if this join clause is an outer join, false otherwise.
     *
     * @throws IllegalStateException if the clause-type is not a join clause.
     *         Only join clauses have a join type.
     */
    public boolean isOuterJoin() {
        if (clauseType != ClauseType.JOIN_EXPR) {
            throw new IllegalStateException(
                "This from-clause is not a join expression.");
        }

        return (joinType == JoinType.LEFT_OUTER ||
                joinType == JoinType.RIGHT_OUTER ||
                joinType == JoinType.FULL_OUTER);
    }


    public boolean hasOuterJoinOnLeft() {
        if (clauseType != ClauseType.JOIN_EXPR) {
            throw new IllegalStateException(
                "This from-clause is not a join expression.");
        }

        return joinType == JoinType.LEFT_OUTER ||
               joinType == JoinType.FULL_OUTER;
    }


    public boolean hasOuterJoinOnRight() {
        if (clauseType != ClauseType.JOIN_EXPR) {
            throw new IllegalStateException(
                "This from-clause is not a join expression.");
        }

        return joinType == JoinType.RIGHT_OUTER ||
               joinType == JoinType.FULL_OUTER;
    }



    /**
     * Returns the join condition type.  This value will be null if there is
     * only 1 table in the outer array of from clauses.
     *
     * @return the join condition type
     *
     * @throws IllegalStateException if the clause-type is not a join clause.
     *         Only join clauses have a join-condition type.
     */
    public JoinConditionType getConditionType() {
        if (clauseType != ClauseType.JOIN_EXPR) {
            throw new IllegalStateException(clauseType +
                " clauses don't have a join-condition type");
        }

        return condType;
    }


    /**
     * Sets the join condition type.
     *
     * @param type the join condition type
     *
     * @throws IllegalStateException if the clause-type is not a join clause.
     *         Only join clauses have a join-condition type.
     */
    public void setConditionType(JoinConditionType type) {
        if (clauseType != ClauseType.JOIN_EXPR) {
            throw new IllegalStateException(clauseType +
                " clauses don't have a join-condition type");
        }

        condType = type;
    }


    /**
     * For join conditions of the form <tt>USING (col1, col2, ...)</tt>, this
     * method returns the list of column names that were specified.  If this
     * join expression does not use a <tt>USING</tt> expression then this method
     * returns <tt>null</tt>.
     *
     * @return the list of column names specified in the <tt>USING</tt> clause
     *
     * @throws IllegalStateException if the clause-type is not a join clause.
     *         Only join clauses have a join condition.
     */
    public List<String> getUsingNames() {
        if (clauseType != ClauseType.JOIN_EXPR) {
            throw new IllegalStateException(clauseType +
                " clauses don't have a join condition");
        }

        return joinUsingNames;
    }


    /**
     * For join conditions of the form <tt>USING (col1, col2, ...)</tt>, this
     * method allows the column names to be added to this from clause.  This
     * method is used by the parser to construct the from clause as it is being
     * parsed.
     *
     * @param name the name of the column to use in the join operation
     *
     * @throws IllegalStateException if the clause-type is not a join clause.
     *         Only join clauses have a join condition.
     */
    public void addUsingName(String name) {
        if (clauseType != ClauseType.JOIN_EXPR) {
            throw new IllegalStateException(clauseType +
                " clauses don't have a join condition");
        }

        if (name == null)
            throw new NullPointerException("name cannot be null");

        joinUsingNames.add(name);
    }


    /**
     * Returns the join predicate if this from clause specifies an <tt>ON</tt>
     * clause.  Returns <tt>null</tt> if there is no <tt>ON</tt> clause.
     *
     * @return the expression specified in the <tt>ON</tt> clause
     *
     * @throws IllegalStateException if the clause-type is not a join clause.
     *         Only join clauses have a join condition.
     */
    public Expression getOnExpression() {
        if (clauseType != ClauseType.JOIN_EXPR) {
            throw new IllegalStateException(clauseType +
                " clauses don't have a join condition");
        }

        return joinOnExpr;
    }


    /**
     * Sets the join predicate if this from clause specifies an <tt>ON</tt>
     * clause.  Returns <tt>null</tt> if there is no <tt>ON</tt> clause.
     *
     * @param expr the expression specified in the <tt>ON</tt> clause
     *
     * @throws IllegalStateException if the clause-type is not a join clause.
     *         Only join clauses have a join condition.
     */
    public void setOnExpression(Expression expr) {
        if (clauseType != ClauseType.JOIN_EXPR) {
            throw new IllegalStateException(clauseType +
                " clauses don't have a join condition");
        }

        joinOnExpr = expr;
    }


    /**
     * This function prepares the from-clause for use by the planner to
     * generate an actual execution plan from the clause.  This includes
     * several important tasks:
     * <ul>
     *   <li>The schema of the from-clause's output is determined.</li>
     *   <li>If the from-clause is a join, and the join specifies
     *       <tt>NATURAL</tt>, <tt>USING (col, ...)</tt>, or an <tt>ON</tt>
     *       clause, the join condition is determined</li>
     * </ul>
     * <p>
     * Since a from-clause can be a SQL subquery or a join expression, it
     * should be evident that the method will recursively prepare its children
     * as well.
     * </p>
     * <p>
     * <b>This method really shouldn't be called directly.</b>  Instead, the
     * enclosing {@link SelectClause} will calls this method when the clause's
     * {@link SelectClause#computeSchema} method is invoked.
     * </p>
     *
     * @return the schema of the from-clause's output
     *
     * @throws IOException if a table's schema cannot be loaded, or some other
     *         IO issue occurs.
     */
    public Schema computeSchema(TableManager tableManager) throws IOException {

        Schema result = null;

        switch (clauseType) {

        case BASE_TABLE:
            logger.debug("Preparing BASE_TABLE from-clause.");

            TableInfo tableInfo = tableManager.openTable(tableName);
            result = tableInfo.getSchema();

            if (aliasName != null) {
                // Make a copy of the result schema and change the table names.
                result = new Schema(result);
                result.setTableName(aliasName);
            }

            break;

        case SELECT_SUBQUERY:
            logger.debug("Preparing SELECT_SUBQUERY from-clause.");

            // Don't pass in a parent SELECT, since we won't allow correlated
            // subqueries in the FROM clause.  (Donnie's still not entirely
            // sure this is possible in standard SQL.)
            result = derivedTable.computeSchema(tableManager, null);

            assert aliasName != null;

            // Make a copy of the result schema and change the table names.
            result = new Schema(result);
            result.setTableName(aliasName);

            break;

        case JOIN_EXPR:
            logger.debug("Preparing JOIN_EXPR from-clause.  Condition type = " +
                condType);

            result = new Schema();

            Schema leftSchema = leftChild.computeSchema(tableManager);
            Schema rightSchema = rightChild.computeSchema(tableManager);

            // Depending on the join type, we might eliminate duplicate column
            // names.
            if (condType == JoinConditionType.NATURAL_JOIN) {
                // Make sure that each side of the join doesn't have any
                // duplicate column names.  (A schema can have columns with the
                // same name, but from different table names.  This is not
                // allowed for natural joins.)
                if (leftSchema.hasMultipleColumnsWithSameName()) {
                    throw new SchemaNameException("Natural join error:  " +
                        "left child table has multiple columns with same " +
                        "column name");
                }
                if (rightSchema.hasMultipleColumnsWithSameName()) {
                    throw new SchemaNameException("Natural join error:  " +
                        "right child table has multiple columns with same " +
                        "column name");
                }

                Set<String> commonCols = leftSchema.getCommonColumnNames(rightSchema);
                if (commonCols.isEmpty()) {
                    // TODO:  According to the SQL99 standard, this shouldn't
                    //        generate an error.
                    throw new SchemaNameException("Natural join error:  " +
                        "child tables share no common column names!");
                }

                buildJoinSchema("Natural join", leftSchema, rightSchema,
                    commonCols, result);
            }
            else if (condType == JoinConditionType.JOIN_USING) {
                LinkedHashSet<String> commonCols = new LinkedHashSet<String>();
                for (String name : joinUsingNames) {
                    if (!commonCols.add(name)) {
                        throw new SchemaNameException("Column name " + name +
                            " was specified multiple times in USING clause");
                    }
                }

                buildJoinSchema("Join USING", leftSchema, rightSchema,
                    commonCols, result);
            }
            else {
                // This join condition-type doesn't alter the result schema.
                // Just lump together the result schemas.
                result.append(leftSchema);
                result.append(rightSchema);

                computedJoinExpr = joinOnExpr;
            }
        }

        // Don't need to do any schema validation in this function, since all the
        // validation takes place in the SELECT clause schema-computation code.

        computedSchema = result;
        return result;
    }


    /**
     * <p>
     * This helper function handles a <tt>NATURAL</tt> join or a join with a
     * <tt>USING</tt> clause, determining what columns to use in the join,
     * computing the join predicate to use, and computing the schema of the
     * join result.
     * </p>
     * <p>
     * The schema of the result follows the SQL standard:
     * </p>
     * <ul>
     *     <li>Common columns first, in the order specified, if any.</li>
     *     <li>Columns that appear only in the left table, if any.</li>
     *     <li>Columns that appear only in the right table, if any.</li>
     * </ul>
     * <p>
     * Of course, any of these sets of columns could be empty.
     * </p>
     *
     * @param context a string used in logging messages and exceptions to
     *        indicate the context in which this method was called.
     *
     * @param leftSchema the schema of the table on the left side of the join
     *
     * @param rightSchema the schema of the table on the right side of the join
     *
     * @param commonCols the set of common columns to use
     * @param result this output-parameter is updated to hold the schema of the
     *        result, as produced by the join operation.
     */
    private void buildJoinSchema(String context, Schema leftSchema,
        Schema rightSchema, Set<String> commonCols, Schema result) {

        computedJoinExpr = null;
        computedSelectValues = null;
        if (!commonCols.isEmpty()) {
            computedSelectValues = new ArrayList<SelectValue>();

            // We will need to generate a join expression using the common
            // columns.  We will also need a project-spec that will project down
            // to only one copy of the common columns.

            BooleanOperator andOp = new BooleanOperator(BooleanOperator.Type.AND_EXPR);

            // Handle the shared columns.  We need to check that the
            // names aren't ambiguous on one or the other side.
            for (String name: commonCols) {
                checkJoinColumn(context, "left", leftSchema, name);
                checkJoinColumn(context, "right", leftSchema, name);

                // Seems OK, so add it to the result.
                ColumnInfo lhsColInfo = leftSchema.getColumnInfo(name);
                ColumnInfo rhsColInfo = rightSchema.getColumnInfo(name);

                result.addColumnInfo(
                    new ColumnInfo(lhsColInfo.getName(), lhsColInfo.getType()));

                // Add an equality test between the common columns to the join
                // condition.
                CompareOperator eq = new CompareOperator(CompareOperator.Type.EQUALS,
                    new ColumnValue(lhsColInfo.getColumnName()),
                    new ColumnValue(rhsColInfo.getColumnName()));

                andOp.addTerm(eq);

                // Add a select-value that projects the appropriate source
                // column down to the common column.
                SelectValue selVal;
                switch (joinType) {
                case INNER:
                case LEFT_OUTER:
                    // We can use the left column in the result, as it will
                    // always be non-NULL.
                    selVal = new SelectValue(
                        new ColumnValue(lhsColInfo.getColumnName()), name);
                    computedSelectValues.add(selVal);
                    break;

                case RIGHT_OUTER:
                    // We can use the right column in the result, as it will
                    // always be non-NULL.
                    selVal = new SelectValue(
                        new ColumnValue(rhsColInfo.getColumnName()), name);
                    computedSelectValues.add(selVal);
                    break;

                case FULL_OUTER:
                    // In this case, the LHS column-value could be null, or the
                    // RHS column-value could be null.  Thus, we need to produce
                    // a result of COALESCE(lhs.col, rhs.col) AS col.
                    Expression coalesce = new FunctionCall("COALESCE",
                        /* distinct */ false,
                        new ColumnValue(lhsColInfo.getColumnName()),
                        new ColumnValue(rhsColInfo.getColumnName()));
                    selVal = new SelectValue(coalesce, name);
                    computedSelectValues.add(selVal);
                }
            }

            computedJoinExpr = andOp;
        }

        // Handle the non-shared columns
        for (ColumnInfo colInfo : leftSchema) {
            if (!commonCols.contains(colInfo.getName())) {
                result.addColumnInfo(colInfo);

                SelectValue selVal = new SelectValue(
                    new ColumnValue(colInfo.getColumnName()), null);
                computedSelectValues.add(selVal);
            }
        }
        for (ColumnInfo colInfo : rightSchema) {
            if (!commonCols.contains(colInfo.getName())) {
                result.addColumnInfo(colInfo);

                SelectValue selVal = new SelectValue(
                    new ColumnValue(colInfo.getColumnName()), null);
                computedSelectValues.add(selVal);
            }
        }
    }


    private void checkJoinColumn(String context, String side, Schema schema,
                                 String colName) {
        int count = schema.numColumnsWithName(colName);
        if (count == 0) {
            throw new SchemaNameException(context + " error:  column name \"" +
                colName + "\" doesn't appear on " + side);
        }
        else if (count > 1) {
            throw new SchemaNameException(context + " error:  column name \"" +
                colName + "\" is ambiguous on " + side);
        }
    }


    public Schema getSchema() {
        return computedSchema;
    }


    public Expression getComputedJoinExpr() {
        return computedJoinExpr;
    }


    public ArrayList<SelectValue> getComputedSelectValues() {
        return computedSelectValues;
    }


    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("JoinClause[type=").append(clauseType);
        switch (clauseType) {
        case BASE_TABLE:
            buf.append(", table=").append(tableName);
            break;

        case JOIN_EXPR:
            buf.append(", joinType=").append(joinType);
            buf.append(", condType=").append(condType);

            if (condType != null) {
                switch (condType) {
                case JOIN_ON_EXPR:
                    buf.append(", onExpr=").append(joinOnExpr);
                    break;

                case JOIN_USING:
                    buf.append(", usingNames=").append(joinUsingNames);
                    buf.append(", computedJoinExpr=").append(computedJoinExpr);
                    break;

                case NATURAL_JOIN:
                    buf.append(", computedJoinExpr=").append(computedJoinExpr);
                }
            }

            buf.append(", leftChild=").append(leftChild);
            buf.append(", rightChild=").append(rightChild);

            break;

        case SELECT_SUBQUERY:
            buf.append(", select=").append(derivedTable);
        }

        if (isRenamed())
            buf.append(", alias=").append(aliasName);

        buf.append(']');
        return buf.toString();
    }
}
