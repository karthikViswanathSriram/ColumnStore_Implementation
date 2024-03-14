package interfaces;

import columnar.Columnarfile;
import global.AttrOperator;
import global.AttrType;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;

import java.util.ArrayList;
import java.util.List;

public class HelperFunctions {

    public static CondExpr[] getCondExpr(String expression, Columnarfile cf) {
        CondExpr[] condExprs;

        if (expression.length() == 0) 
        {
            condExprs = new CondExpr[1];
            condExprs[0] = null;

            return condExprs;
        }

        String[] andExpressions = expression.split(" \\^ ");
        condExprs = new CondExpr[andExpressions.length + 1];
        for (int i = 0; i < andExpressions.length; i++) 
        {
            String temp = andExpressions[i].replace("(", "");
            temp = temp.replace(")", "");
            String[] orExpressions = temp.split(" v ");

            condExprs[i] = new CondExpr();
            CondExpr condExpr = condExprs[i];
            for (int j = 0; j < orExpressions.length; j++) {
                String singleExpression = orExpressions[j].replace("[", "");
                singleExpression = singleExpression.replace("]", "");
                String[] expressionParts = singleExpression.split(" ");
                String attributeName = getAttributeName(expressionParts[0]);
                String stringOperator = expressionParts[1];
                String attributeValue = expressionParts[2];

                condExpr.type1 = new AttrType(AttrType.attrSymbol);
                condExpr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), cf.getAttributePosition(attributeName) + 1);
                condExpr.op = getOperatorForString(stringOperator);
                if (isInteger(attributeValue)) {
                    condExpr.type2 = new AttrType(AttrType.attrInteger);
                    condExpr.operand2.integer = Integer.parseInt(attributeValue);
                } else if (isString(attributeValue)) {
                    condExpr.type2 = new AttrType(AttrType.attrString);
                    condExpr.operand2.string = attributeValue.replace("'","");
                } else {
                    condExpr.type2 = new AttrType(AttrType.attrSymbol);
                    String name = getAttributeName(attributeValue);
                    condExpr.operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), cf.getAttributePosition(name) + 1);
                }

                if (j == orExpressions.length - 1) {
                    condExpr.next = null;
                } else {
                    condExpr.next = new CondExpr();
                    condExpr = condExpr.next;
                }
            }
        }
        condExprs[andExpressions.length] = null;

        return condExprs;
    }

    public static CondExpr[] getCondExpr(String expression) {
        // Sample input
        // String expression = "([columnarTable1.A = 'RandomTextHere'] v [columnarTable1.B > 2]) ^ ([columnarTable1.C = columnarTable1.D])"
        CondExpr[] condExprs;

        if (expression.length() == 0) {
            condExprs = new CondExpr[1];
            condExprs[0] = null;

            return condExprs;
        }

        String[] andExpressions = expression.split(" \\^ ");
        condExprs = new CondExpr[andExpressions.length + 1];
        for (int i = 0; i < andExpressions.length; i++) {
            String temp = andExpressions[i].replace("(", "");
            temp = temp.replace(")", "");
            String[] orExpressions = temp.split(" v ");

            condExprs[i] = new CondExpr();
            CondExpr condExpr = condExprs[i];
            for (int j = 0; j < orExpressions.length; j++) {
                String singleExpression = orExpressions[j].replace("[", "");
                singleExpression = singleExpression.replace("]", "");
                String[] expressionParts = singleExpression.split(" ");
                String stringOperator = expressionParts[1];
                String attributeValue = expressionParts[2];

                condExpr.type1 = new AttrType(AttrType.attrSymbol);
                condExpr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                condExpr.op = getOperatorForString(stringOperator);
                if (isInteger(attributeValue)) {
                    condExpr.type2 = new AttrType(AttrType.attrInteger);
                    condExpr.operand2.integer = Integer.parseInt(attributeValue);
                } else if (isString(attributeValue)) {
                    condExpr.type2 = new AttrType(AttrType.attrString);
                    condExpr.operand2.string = attributeValue.replace("'","");
                } else {
                    condExpr.type2 = new AttrType(AttrType.attrSymbol);
                    condExpr.operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
                }

                if (j == orExpressions.length - 1) {
                    condExpr.next = null;
                } else {
                    condExpr.next = new CondExpr();
                    condExpr = condExpr.next;
                }
            }
        }
        condExprs[andExpressions.length] = null;

        return condExprs;
    }

    public static CondExpr[] processEquiJoinConditionExpression(String expression, String[] innerTargetColumns, String[] outerTargetColumns) throws Exception {
        CondExpr[] condExprs;

        if (expression.length() == 0) {
            condExprs = new CondExpr[1];
            condExprs[0] = null;

            return condExprs;
        }

        String[] andExpressions = expression.split(" \\^ ");
        condExprs = new CondExpr[andExpressions.length + 1];
        for (int i = 0; i < andExpressions.length; i++) {
            String temp = andExpressions[i].replace("(", "");
            temp = temp.replace(")", "");
            String[] orExpressions = temp.split(" v ");

            condExprs[i] = new CondExpr();
            CondExpr condExpr = condExprs[i];
            for (int j = 0; j < orExpressions.length; j++) {
                String singleExpression = orExpressions[j].replace("[", "");
                singleExpression = singleExpression.replace("]", "");
                String[] expressionParts = singleExpression.split(" ");
                String attribute1Name = expressionParts[0].split("\\.")[1];
                String stringOperator = expressionParts[1];
                String attribute2Name = expressionParts[2].split("\\.")[1];

                condExpr.type1 = new AttrType(AttrType.attrSymbol);
                condExpr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), getPositionTargetColumns(attribute1Name, outerTargetColumns) + 1);
                condExpr.op = getOperatorForString(stringOperator);
                condExpr.type2 = new AttrType(AttrType.attrSymbol);
                condExpr.operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), getPositionTargetColumns(attribute2Name, innerTargetColumns) + 1);

                if (j == orExpressions.length - 1) {
                    condExpr.next = null;
                } else {
                    condExpr.next = new CondExpr();
                    condExpr = condExpr.next;
                }
            }
        }
        condExprs[andExpressions.length] = null;

        return condExprs;
    }

    public static CondExpr[] getCondExpr(String expression, String[] targetColumns) throws Exception {
        // Sample input
        // String expression = "([columnarTable1.A = 'RandomTextHere'] v [columnarTable1.B > 2]) ^ ([columnarTable1.C = columnarTable1.D])"
        CondExpr[] condExprs;

        if (expression.length() == 0) {
            condExprs = new CondExpr[1];
            condExprs[0] = null;

            return condExprs;
        }

        String[] andExpressions = expression.split(" \\^ ");
        condExprs = new CondExpr[andExpressions.length + 1];
        for (int i = 0; i < andExpressions.length; i++) {
            String temp = andExpressions[i].replace("(", "");
            temp = temp.replace(")", "");
            String[] orExpressions = temp.split(" v ");

            condExprs[i] = new CondExpr();
            CondExpr condExpr = condExprs[i];
            for (int j = 0; j < orExpressions.length; j++) {
                String singleExpression = orExpressions[j].replace("[", "");
                singleExpression = singleExpression.replace("]", "");
                String[] expressionParts = singleExpression.split(" ");
                String attributeName = getAttributeName(expressionParts[0]);
                String stringOperator = expressionParts[1];
                String attributeValue = expressionParts[2];

                condExpr.type1 = new AttrType(AttrType.attrSymbol);
                condExpr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), getPositionTargetColumns(attributeName, targetColumns) + 1);
                condExpr.op = getOperatorForString(stringOperator);
                if (isInteger(attributeValue)) {
                    condExpr.type2 = new AttrType(AttrType.attrInteger);
                    condExpr.operand2.integer = Integer.parseInt(attributeValue);
                } else if (isString(attributeValue)) {
                    condExpr.type2 = new AttrType(AttrType.attrString);
                    condExpr.operand2.string = attributeValue.replace("'","");
                } else {
                    condExpr.type2 = new AttrType(AttrType.attrSymbol);
                    String name = getAttributeName(attributeValue);
                    condExpr.operand2.symbol = new FldSpec(new RelSpec(RelSpec.outer), getPositionTargetColumns(name, targetColumns) + 1);
                }

                if (j == orExpressions.length - 1) {
                    condExpr.next = null;
                } else {
                    condExpr.next = new CondExpr();
                    condExpr = condExpr.next;
                }
            }
        }
        condExprs[andExpressions.length] = null;

        return condExprs;
    }

    private static Boolean isString(String value) {
        if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
            return Boolean.TRUE;
        } else {
            return Boolean.FALSE;
        }
    }

    public static String getAttributeName(String value) {
        String[] val = value.split("\\.");
        return val.length == 2? val[1]:val[0];
    }

    private static AttrOperator getOperatorForString(String operator) {
        switch (operator) {
            case "=":
                return new AttrOperator(AttrOperator.aopEQ);
            case ">":
                return new AttrOperator(AttrOperator.aopGT);
            case "<":
                return new AttrOperator(AttrOperator.aopLT);
            case "!=":
                return new AttrOperator(AttrOperator.aopNE);
            case "<=":
                return new AttrOperator(AttrOperator.aopLE);
            case ">=":
                return new AttrOperator(AttrOperator.aopGE);
        }

        return null;
    }

    public static boolean isInteger(String s) {
        try {
            Integer intValue = Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static int getPositionTargetColumns(String columnName, String[] targetColumns) throws Exception {

        for(int i = 0; i < targetColumns.length; i++){
            if(columnName.equals(targetColumns[i]))
                return i;
        }
        throw new Exception(columnName + " not found in targets");
    }

    public static String dbPath(String columnDB){
        // Hardcoding the path
        String path = "dbs/CSE510_DBMSI_GROUP4_";
        return path + columnDB + ".minibaseDB";
    }


    public static List<String> getSerializedConditionList(CondExpr[] condExprs) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < condExprs.length - 1; i++) {
            CondExpr temp = condExprs[i];
            while (temp != null && temp.next != null) {
                temp = temp.next;
                result.add("OR");
            }
            if (condExprs[i + 1] != null) {
                result.add("AND");
            }
        }

        return result;
    }

}