import torch
import torch.nn


from data_handler import *
from MyModel.EncoderBiLSTMDNN_five_classify import *
from MyModel.EncoderBiLSTMDNN_two_classify import *
from MyModel.MultiAttentiomBiLSTMDNN_five_classify import *
from MyModel.MultiAttentiomBiLSTMDNN_two_classify import *
from MyModel.BiLSTMDNN_two_classify import BiLSTMDNN_Two_Classify
from MyModel.BiLSTMDNN_five_classify import BiLSTMDNN_Five_Classify
from MyModel.PositionEncoderDNN_two_classify import PositionEncoderDNN_Two_Classify
from MyModel.TransformerDNN_five_classify import EncoderDNN_Five_Classify
from MyModel.TransformerDNN_two_classify import EncoderDNN_Two_Classify
from MyModel.PositionEncoderDNN_five_classify import PositionEncoderDNN_Five_Classify
import torch.optim as optimizer
import torch.utils.data as data
from config import *


def get_2_dataLoader(classify=5):
    X_train, X_test, Y_train, Y_test = get_NSLKDD(classify)
    print("Training data _ shape: ", X_test.shape[0]);
    if X_test.shape[0] == Y_test.shape[0]:
        print('Total size of train data：', X_test.size(), "Total size of test data：", Y_test.size())
    
    test_dataset = data.TensorDataset(X_test, Y_test)
    test_loader = data.DataLoader(
        dataset=test_dataset,
        batch_size=BatchSize,
        shuffle=False
    )

    X_train = pd.read_csv("data2\Xtrain_gen.csv", header=None, index_col=None)
    Y_train = pd.read_csv("data2\Ytrain_gen.csv", header=None, index_col=None)
    if classify==2:
        Y_train[Y_train!=0]=1
    X_train = torch.FloatTensor(X_train.to_numpy()).unsqueeze(dim=-1)
    Y_train = torch.LongTensor(Y_train.to_numpy().squeeze())
    if classify==5:
        printdict = {}
        print("train_data,total:" + str(Y_train.size()[0]))
        for k, v in labels.items():
            num = torch.eq(Y_train, v).sum().cpu().detach().numpy()
            # print(k + ":" + str(num) + ":" + str(num / Y_test.size()[0]))
            printdict[k] = int(num)
        print(printdict)

    train_dataset = torch.utils.data.TensorDataset(X_train, Y_train)
    train_loader = torch.utils.data.DataLoader(
        dataset=train_dataset,
        batch_size=BatchSize,
        shuffle=True
    )
    return train_loader, test_loader, (X_train, X_test, Y_train, Y_test)







def train_model_five_classify(model, count=0):
    train_loader, test_loader, (X_train, X_test, Y_train, Y_test) = get_2_dataLoader(FiveClassify)
    # train_loader, test_loader, (X_train, X_test, Y_train, Y_test) = get_dataset_loader(FiveClassify)

    my_model = model.to(Device)
    if (IsInitParams):
        initNetParams(my_model)

    optim = optimizer.Adam(my_model.parameters(), lr=FiveClassifyLR)
    #    scheduler = optimizer.lr_scheduler.StepLR(optim,step_size=20,gamma=0.1)
    scheduler = optimizer.lr_scheduler.MultiStepLR(optim, milestones=FiveClassifyMilestones, gamma=0.1)
    loss_func = nn.CrossEntropyLoss().to(Device)

    print("Model Details：---------------------------------------------------------------")
    print(my_model)
    print("Epoch：%d,epoch:%d,batch_size:%d, loss function: %s,optim : %s" % (
        FiveClassify, FiveClassifyEpoch, BatchSize, loss_func.__str__(), optim.__str__()))
    print("--------------------------------------------------------------")
    save_acc = 0
    test_acc_array = []
    train_acc_array = []
    test_loss_array = []
    train_loss_array = []
    for epoch in range(5):
        train_loss, train_acc = train(train_loader, my_model, loss_func, optim)
        # test_loss, test_acc, test_pred = test_report(test_loader, my_model, loss_func)
        print(
            "Epoch:%d => train_loss:%.10f => train_acc:%.10f%% =>lr: %.10f" % (
                epoch + 1, train_loss, train_acc * 100,
                optim.state_dict()['param_groups'][0]['lr']))

        scheduler.step()
        
        train_loss_array.append(train_loss)
        
    torch.save(my_model.state_dict(), "trained.pt")

    
    


def train(train_loader, model, loss_func, optimizer):
    train_loss = 0
    num_correct = 0
    print(model)
    for step, (batch_x, batch_y) in enumerate(train_loader):
        batch_x, batch_y = batch_x.to(Device), batch_y.to(Device)
        model.train()
        prediction = model(batch_x)
        loss = loss_func(prediction, batch_y)

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        train_loss += loss.cpu().detach().numpy()
        pred = prediction.argmax(dim=1)
        num_correct += torch.eq(pred, batch_y).sum().cpu().detach().numpy()
    train_loss, train_acc = train_loss / len(train_loader), num_correct / len(train_loader.dataset)
    return train_loss, train_acc



train_model_five_classify(MultiAttentionBiLSTMDNN_Five_Classify())


