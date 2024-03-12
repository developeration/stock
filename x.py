import cv2


from tkinter import *
from PIL import Image, ImageTk
import time
import traceback
from skimage.metrics import structural_similarity as ssim
import numpy as np
from pywinauto.keyboard import send_keys
from selenium.webdriver.common.keys import Keys

window=Tk()
window.title('XS')
window.geometry('600x400')
image_width = 500
image_height = 300
capture = cv2.VideoCapture(0)
canvas = Canvas(window,bg = 'white',width = image_width,height = image_height )
selection_start = None
selection_end = None
pilImage = None


def start_selection(event):
    global selection_start, selection_end
    selection_end = None
    selection_start = (event.x, event.y)

def end_selection(event):
    global selection_end
    selection_end = (event.x, event.y)


canvas.bind("<Button-1>", start_selection)
canvas.bind("<ButtonRelease-1>", end_selection)


image_start = None
def btn_start_callback():
    global selection_start, selection_end,image_start,pilImage
    if selection_start and selection_end:
        x1, y1 = selection_start
        x2, y2 = selection_end
        image = pilImage.copy()
        image.load()
        image = image.resize((image_width,image_height))
        image_start = image.crop((x1, y1,x2,y2))
        lab_information['text']='Start.'
    else:
        print("No Selected Rect")


def btn_test_callback():
    # global pilImage
    # _image = pilImage.copy()        
    global image_start
    image_start=None
    lab_information['text']='End.'
    # canvas.postscript( file="C:\\Work\\HideDesktop\\canvas_start.ps", colormode="color")
    # image = Image.open("C:\\Work\\HideDesktop\\canvas_start.ps")
    # image.load()
    # image = image.resize((image_width,image_height))
    # print(np.array(image))
    # image_start = image.crop((5, 5,200,200))
    # print(image_start)
    # print(np.array(image_start))



btn_start = Button(window, text ="Start", command = btn_start_callback)
btn_end = Button(window, text ="End", command = btn_test_callback)
lab_information = Label(window, text=".")
while(True):
    try:
        ret, frame = capture.read()
        frame = cv2.flip(frame, 1)
        cvimage = cv2.cvtColor(frame, cv2.COLOR_BGR2RGBA)
        pilImage = Image.fromarray(cvimage)
        pilImage = pilImage.resize((image_width, image_height),Image.ANTIALIAS)
        tkImage =  ImageTk.PhotoImage(image=pilImage)  
        
        canvas.create_image(0,0,anchor = 'nw',image = tkImage)
        canvas.place(x = 0,y = 0)

        if selection_start and selection_end:
            x1, y1 = selection_start
            x2, y2 = selection_end
            canvas.create_rectangle((x1, y1, x2, y2), fill=None, dash=1, width=1, outline='red') 
            #print("选择区域的尺寸为：{}x{}".format(width, height))
            

        btn_start.place(x=505,y=10)
        btn_end.place(x=505,y=150)
        lab_information.place(x=10,y=310)
        


        
        

        if selection_start and selection_end and image_start  :
            #canvas.postscript( file="C:\\Work\\HideDesktop\\canvas_process.ps", colormode="color")
            #image = Image.open("C:\\Work\\HideDesktop\\canvas_process.ps")
            image = pilImage.copy()
            image.load()
            image = image.resize((image_width,image_height))
            x1, y1 = selection_start
            x2, y2 = selection_end
            image_process = image.crop((x1, y1,x2,y2))
            image_start_ssim = cv2.cvtColor(np.array(image_start),cv2.COLOR_BGR2GRAY)
            image_process_ssim = cv2.cvtColor(np.array(image_process),cv2.COLOR_BGR2GRAY)
            ssim_value = ssim(np.array(image_start_ssim),np.array(image_process_ssim) ,multichannel=True)
            
            #print(ssim_value)
            if(ssim_value<0.85):
                send_keys('{VK_LWIN down}m{VK_LWIN up}')
                capture.release()
                window.quit()
                break


        window.update()
        window.after(1)
        #time.sleep(5)

    except Exception as ex:
        capture.release()
        window.quit()
        traceback.print_exc()
        break


