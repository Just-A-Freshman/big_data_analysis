import pandas as pd
from os import path
from sklearn.linear_model import LinearRegression


ROOT_PATH = "../dataset"
# 启用未来设置的选项
pd.set_option('future.no_silent_downcasting', True)


class IndustryPrediction(object):
    # 希望能通过已有的正确的position,require和company_type字段联合预测出缺失的industry
    def __init__(self):
        self.feature_columns = ['position', 'company_type', 'require']
        self.target_column = 'industry'

    def read_train_data(self):
        # 读取需要的数据
        dtype_map = {
            'position': 'category',
            'company_type': 'category',
            'require': 'category',
            'industry': 'category'
        }
        df = pd.read_csv('../dataset/job.csv', dtype=dtype_map)
        df.dropna(subset="position", inplace=True)
        df.dropna(subset="company_type", inplace=True)
        df.dropna(subset="require", inplace=True)
        df.dropna(subset="industry", inplace=True)
        # 数据准备
        feature_x = df[self.feature_columns]
        target_y = df[self.target_column]
        return feature_x, target_y

    def train(self):
        # 写了一坨，还是干脆搁置吧[捂脸]
        pass

    def predict(self):
        # 暂时不会写以及训练，留坑
        pass


class SalaryPredict(object):
    def __init__(self):
        self.education_tuple: tuple = ('小学', '初中', '中专', '高中', '大专', '本科', '硕士', '博士')
        self.train_data: pd.DataFrame = pd.read_csv(path.join(ROOT_PATH, "train.csv"))
        self.preprocessing()

    def preprocessing(self):
        # 工作经验映射:
        work_experience_map = {
            "无": 0, "一年": 1, "二年": 2, "三年": 3, "四年": 4, "五年": 5,
            "六年": 6, "七年": 7, "八年": 8, "九年": 9, "十年": 10,
            "十一年": 11, "十二年": 12, "十五年": 15
        }

        self.train_data["work_experience"] = self.train_data["work_experience"].replace(work_experience_map)

    def train(self):
        for education in self.education_tuple:
            model = self._train(education)
            self.predict(model)

    def _train(self, education):
        boolean_df: pd.DataFrame = self.train_data['education'] == education
        train = self.train_data[boolean_df].to_numpy()
        # 使用连续切片能保证数组的维度一致，而使用索引会使维度下降
        x = train[:, 1: 2]
        y = train[:, 2]
        # model 训练
        model = LinearRegression()
        model.fit(x, y)
        return model

    @staticmethod
    def predict(model):
        # model 预测
        # 尽管我们主观上认为model.score()没必要传递参数，因为可以重用前面训练时传递进去的x, y
        # 但为了清晰性以及更广泛的需求(比如只使用一部分真实数据训练出结果，然后给一个更广泛的准确数据进行误差评估)
        predict_x = [[i] for i in range(11)]
        print(f"预测结果: {model.predict(predict_x)}")


if __name__ == "__main__":
    salary_predict = SalaryPredict()
    salary_predict.train()
