package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.Transformer;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class SysPropertyTransformer extends Transformer {
    public SysPropertyTransformer() {
        setTransformerName("dx_sys_property");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        int columnIndex;
        String propertyKey;
        String propertyValue = null;
        try {
            columnIndex = (Integer) paras[0];
            if (columnIndex < 0) {
                columnIndex = record.getColumnNumber() - 1;
            }
            propertyKey = (String) paras[1];
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                    "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        if (StringUtils.isEmpty(propertyKey)) {
            return record;
        }
        propertyValue = System.getProperty(propertyKey);
        try {
            record.setColumn(columnIndex, new StringColumn(propertyValue));
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
