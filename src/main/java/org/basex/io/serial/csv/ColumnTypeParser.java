package org.basex.io.serial.csv;

import org.h2.engine.Mode;
import org.h2.table.Column;
import org.h2.value.DataType;
import org.h2.value.TypeInfo;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ColumnTypeParser {
    private static final String COLUMN_TYPE_SQL = "(\\w+)(\\((\\d+)(,(\\d+))?\\))?(.*)?";
    private static final int REGEXP_GROUP_COUNT = 6;
    private static final int TYPE_INDEX = 1;
    private static final int PRECISION_INDEX = 3;
    private static final int SCALE_INDEX = 5;
    private static final int IS_NULL_INDEX = 6;
    private static final int DEFAULT_VALUE = -1;
    private static final String NOT_NULL = "NOT NULL";

    private final Pattern pattern = Pattern.compile(COLUMN_TYPE_SQL);

    Column getColumnType(String columnName, String typeString) {
        Matcher matcher = pattern.matcher(typeString);
        if(matcher.matches() && matcher.groupCount()== REGEXP_GROUP_COUNT){
            String type = matcher.group(TYPE_INDEX);
            String precisionStr = matcher.group(PRECISION_INDEX);
            int precision = precisionStr!=null?Integer.parseInt(precisionStr): DEFAULT_VALUE;
            String scaleStr = matcher.group(SCALE_INDEX);
            int scale = scaleStr!=null?Integer.parseInt(scaleStr): DEFAULT_VALUE;
            String isNull = matcher.group(IS_NULL_INDEX);
            DataType typeByName = DataType.getTypeByName(type, Mode.getRegular());
            TypeInfo typeInfo = TypeInfo.getTypeInfo(typeByName.type, precision, scale, null);
            Column column = new Column(columnName, typeInfo);
            if(isNull!=null && isNull.contains(NOT_NULL)){
                column.setNullable(false);
            }
            return column;

        }
        throw new IllegalArgumentException("String '"+typeString+"' is not match regexp: " + COLUMN_TYPE_SQL +
                ". For example valid string is: decimal(20,4) not null");
    }
}
